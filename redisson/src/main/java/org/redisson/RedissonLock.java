/**
 * Copyright (c) 2013-2019 Nikita Koksharov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.redisson;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;

import org.redisson.api.RFuture;
import org.redisson.api.RLock;
import org.redisson.client.RedisException;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.protocol.RedisCommand;
import org.redisson.client.protocol.RedisCommand.ValueType;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.RedisStrictCommand;
import org.redisson.client.protocol.convertor.IntegerReplayConvertor;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.misc.RPromise;
import org.redisson.misc.RedissonPromise;
import org.redisson.pubsub.LockPubSub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.Timeout;
import io.netty.util.TimerTask;

/**
 * Distributed implementation of {@link java.util.concurrent.locks.Lock}
 * Implements reentrant lock.<br>
 * Lock will be removed automatically if client disconnects.
 * <p>
 * Implements a <b>non-fair</b> locking so doesn't guarantees an acquire order.
 *
 * @author Nikita Koksharov
 *
 */
public class RedissonLock extends RedissonExpirable implements RLock {

    public static class ExpirationEntry {
        
        private final Map<Long, Integer> threadIds = new LinkedHashMap<>();
        private volatile Timeout timeout;
        
        public ExpirationEntry() {
            super();
        }
        
        public void addThreadId(long threadId) {
            Integer counter = threadIds.get(threadId);
            if (counter == null) {
                counter = 1;
            } else {
                counter++;
            }
            threadIds.put(threadId, counter);
        }
        public boolean hasNoThreads() {
            return threadIds.isEmpty();
        }
        public Long getFirstThreadId() {
            if (threadIds.isEmpty()) {
                return null;
            }
            return threadIds.keySet().iterator().next();
        }
        public void removeThreadId(long threadId) {
            Integer counter = threadIds.get(threadId);
            if (counter == null) {
                return;
            }
            counter--;
            if (counter == 0) {
                threadIds.remove(threadId);
            } else {
                threadIds.put(threadId, counter);
            }
        }
        
        
        public void setTimeout(Timeout timeout) {
            this.timeout = timeout;
        }
        public Timeout getTimeout() {
            return timeout;
        }
        
    }
    
    private static final Logger log = LoggerFactory.getLogger(RedissonLock.class);
    
    private static final ConcurrentMap<String, ExpirationEntry> EXPIRATION_RENEWAL_MAP = new ConcurrentHashMap<>();
    protected long internalLockLeaseTime;

    final String id;
    final String entryName;

    protected final LockPubSub pubSub;

    final CommandAsyncExecutor commandExecutor;

    public RedissonLock(CommandAsyncExecutor commandExecutor, String name) {
        //调用父类的构造方法 里面没有重要的操作，加入了一个序列化codec，设置了一下树形
        super(commandExecutor, name);
        this.commandExecutor = commandExecutor;
        //随机uuid
        this.id = commandExecutor.getConnectionManager().getId();
        //看门狗watchdog 的租约时间，这个比较重要，因为设计到lock 的续约 与 故障的自动释放锁
        //这里面有一个比较好的设计思想
        //默认30
        this.internalLockLeaseTime = commandExecutor.getConnectionManager().getCfg().getLockWatchdogTimeout();
        this.entryName = id + ":" + name;
        this.pubSub = commandExecutor.getConnectionManager().getSubscribeService().getLockPubSub();
    }

    protected String getEntryName() {
        return entryName;
    }

    String getChannelName() {
        return prefixName("redisson_lock__channel", getName());
    }

    protected String getLockName(long threadId) {
        return id + ":" + threadId;
    }

    @Override
    public void lock() {
        try {
            lock(-1, null, false);
        } catch (InterruptedException e) {
            throw new IllegalStateException();
        }
    }

    @Override
    public void lock(long leaseTime, TimeUnit unit) {
        try {
            lock(leaseTime, unit, false);
        } catch (InterruptedException e) {
            throw new IllegalStateException();
        }
    }


    @Override
    public void lockInterruptibly() throws InterruptedException {
        lock(-1, null, true);
    }

    @Override
    public void lockInterruptibly(long leaseTime, TimeUnit unit) throws InterruptedException {
        lock(leaseTime, unit, true);
    }

    /**
     * 阻塞的获取锁
     * @param leaseTime  加锁以后leaseTime秒钟自动解锁  -1表示不会自动解锁 需要收到解锁
     * @param unit
     * @param interruptibly
     * @throws InterruptedException
     */
    private void lock(long leaseTime, TimeUnit unit, boolean interruptibly) throws InterruptedException {

        //线程id
        long threadId = Thread.currentThread().getId();
        /**
         * 尝试获取锁并返回其他线程持有锁的剩余失效时间
         * 返回Null代表获取锁成功 非null代表获取锁失败 值代表其他线程持有锁的剩余失效时间
         */
        Long ttl = tryAcquire(leaseTime, unit, threadId);
        // 如果返回的失效时间为空，表示锁获取成功(后面看tryAcquire可以知道)
        if (ttl == null) {
            return;
        }

        //加锁失败
        // 使用redis->subscribe订阅channel，用于监听回调处理  释放锁时会发布事件 通知等待锁的线程
        RFuture<RedissonLockEntry> future = subscribe(threadId);
        commandExecutor.syncSubscription(future);

        // 获锁失败，则使用while循环不断获取锁的剩余过期时间ttl，然后指定Park的时间为ttl，不断循环判断
        try {
            while (true) {

                ttl = tryAcquire(leaseTime, unit, threadId);
                // lock acquired
                //获取锁成功
                if (ttl == null) {
                    break;
                }

                // waiting for message
                //锁的过期时间为ttl 其他线程持有锁的过期时间为ttl 当前线程就等ttl时长 再去获取锁
                if (ttl >= 0) {
                    try {
                        //Semaphore.tryAcquire() 信号量为0 等待ttl时长
                        getEntry(threadId).getLatch().tryAcquire(ttl, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        if (interruptibly) {
                            throw e;
                        }
                        getEntry(threadId).getLatch().tryAcquire(ttl, TimeUnit.MILLISECONDS);
                    }
                } else {
                    if (interruptibly) {
                        getEntry(threadId).getLatch().acquire();
                    } else {
                        getEntry(threadId).getLatch().acquireUninterruptibly();
                    }
                }
            }
        } finally {
            unsubscribe(future, threadId);
        }
//        get(lockAsync(leaseTime, unit));
    }

    /**
     * 返回Null代表加锁成功 返回Long代表锁的过期时间
     * @param leaseTime
     * @param unit
     * @param threadId
     * @return
     */
    private Long tryAcquire(long leaseTime, TimeUnit unit, long threadId) {
        return get(tryAcquireAsync(leaseTime, unit, threadId));
    }
    
    private RFuture<Boolean> tryAcquireOnceAsync(long leaseTime, TimeUnit unit, long threadId) {
        if (leaseTime != -1) {
            return tryLockInnerAsync(leaseTime, unit, threadId, RedisCommands.EVAL_NULL_BOOLEAN);
        }
        RFuture<Boolean> ttlRemainingFuture = tryLockInnerAsync(commandExecutor.getConnectionManager().getCfg().getLockWatchdogTimeout(), TimeUnit.MILLISECONDS, threadId, RedisCommands.EVAL_NULL_BOOLEAN);
        ttlRemainingFuture.onComplete((ttlRemaining, e) -> {
            if (e != null) {
                return;
            }

            // lock acquired
            if (ttlRemaining) {
                scheduleExpirationRenewal(threadId);
            }
        });
        return ttlRemainingFuture;
    }

    // 参数1 leaseTime：锁的失效时间(从开始获取锁时计时)
    // 参数2 unit：时间单位
    // 参数3：线程id
    private <T> RFuture<Long> tryAcquireAsync(long leaseTime, TimeUnit unit, long threadId) {

        //Redisson 看门狗（Watchdog）在指定加锁时间(非-1)时，是不会对锁时间自动续租的。
        if (leaseTime != -1) {
            //设置了锁的失效时间
            /**
             * 非公平锁（默认） -> RedissonLock#tryLockInnerAsync()
             * 公平锁 ->  RedissonFairLock#tryLockInnerAsync()
             * 读锁 -> RedissonReadLock#tryLockInnerAsync()
             * 写锁 -> RedissonWriteLock#tryLockInnerAsync()
             */
            return tryLockInnerAsync(leaseTime, unit, threadId, RedisCommands.EVAL_LONG);
        }
        //当leaseTime为-1时启用watchdog 即没有指定锁的失效时间

        //没有设置锁的超时时间 超时时间默认为30s  commandExecutor.getConnectionManager().getCfg().getLockWatchdogTimeout()
        RFuture<Long> ttlRemainingFuture = tryLockInnerAsync(commandExecutor.getConnectionManager().getCfg().getLockWatchdogTimeout(), TimeUnit.MILLISECONDS, threadId, RedisCommands.EVAL_LONG);
        ttlRemainingFuture.onComplete((ttlRemaining, e) -> {
            if (e != null) {
                return;
            }

            // lock acquired
            //获取锁成功 进行续期
            if (ttlRemaining == null) {
                //续期
                scheduleExpirationRenewal(threadId);
            }
        });
        return ttlRemainingFuture;
    }

    @Override
    public boolean tryLock() {
        return get(tryLockAsync());
    }

    private void renewExpiration() {
        ExpirationEntry ee = EXPIRATION_RENEWAL_MAP.get(getEntryName());
        if (ee == null) {
            return;
        }
        
        Timeout task = commandExecutor.getConnectionManager().newTimeout(new TimerTask() {
            @Override
            public void run(Timeout timeout) throws Exception {
                ExpirationEntry ent = EXPIRATION_RENEWAL_MAP.get(getEntryName());
                if (ent == null) {
                    return;
                }
                //取第一个线程
                Long threadId = ent.getFirstThreadId();
                if (threadId == null) {
                    return;
                }
                //Lua脚本延期锁的过期时间
                RFuture<Boolean> future = renewExpirationAsync(threadId);
                future.onComplete((res, e) -> {
                    if (e != null) {
                        log.error("Can't update lock " + getName() + " expiration", e);
                        return;
                    }
                    //延期成功
                    if (res) {
                        // 当续租成功之后，重新调用 renewExpiration 自己，从而达到持续续租的目的
                        renewExpiration();
                    }
                });
            }
            //默认每隔10s检查一次
        }, internalLockLeaseTime / 3, TimeUnit.MILLISECONDS);
        
        ee.setTimeout(task);
    }
    
    private void scheduleExpirationRenewal(long threadId) {
        ExpirationEntry entry = new ExpirationEntry();

        ExpirationEntry oldEntry = EXPIRATION_RENEWAL_MAP.putIfAbsent(getEntryName(), entry);
        //第2次以后再获取锁，不用再使用时间轮算法延期了
        if (oldEntry != null) {
            oldEntry.addThreadId(threadId);
        } else {
            //当前分布式锁还没有watchdog
            entry.addThreadId(threadId);

            //每隔10秒自动续期
            renewExpiration();
        }
    }

    protected RFuture<Boolean> renewExpirationAsync(long threadId) {
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                //如果锁存在 就续约并返回1 否则返回0
                "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
                    "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                    "return 1; " +
                "end; " +
                "return 0;",
            Collections.<Object>singletonList(getName()), 
            internalLockLeaseTime, getLockName(threadId));
    }

    void cancelExpirationRenewal(Long threadId) {
        ExpirationEntry task = EXPIRATION_RENEWAL_MAP.get(getEntryName());
        if (task == null) {
            return;
        }
        
        if (threadId != null) {
            task.removeThreadId(threadId);
        }
        
        if (threadId == null || task.hasNoThreads()) {
            task.getTimeout().cancel();
            EXPIRATION_RENEWAL_MAP.remove(getEntryName());
        }
    }

    <T> RFuture<T> tryLockInnerAsync(long leaseTime, TimeUnit unit, long threadId, RedisStrictCommand<T> command) {
        // 获取锁的有效时间
        internalLockLeaseTime = unit.toMillis(leaseTime);

        //通过EVAL命令执行Lua脚本获取锁，保证了原子性
        /**
         *  锁：hash结构
         *  key - 资源名称 （多个线程竞争这个资源）
         *  filed - 线程标识 threadid + uuid
         *  value - 冲入次数
         */
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, command,
                /**
                 *    KEYS[1]：Collections.<Object>singletonList(getName())就是该分布式锁的key，也就是初始化锁时设置的名字。
                 *    ARGV[1]：internalLockLeaseTime，就是锁的有效时间 即线程持有锁的时长最长为leaseTime
                 *    ARGV[2]：getLockName(threadId)，就是hash结构里的key，即UUID+threadId。 (注意 UUID是随机的)
                 */
                /**
                 *    如果key不存在，则执行hset命令,hset 是对于redis map数据结构 其实就是针对key map 中的某个数据 设置为1
                 *    然后通过pexpire命令设置锁的过期时间 返回空值nil，表示获取锁成功
                 *    "lockName":{
                 * 	           //uuid(客户端 id) + threadID:重入次数加了1
                 * 	           "11bb52bc-a764-4649-8b46-a61513d7fe44:1":2
                 *       }
                 */
                  "if (redis.call('exists', KEYS[1]) == 0) then " +
                      "redis.call('hset', KEYS[1], ARGV[2], 1); " +
                      "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                      "return nil; " +
                  "end; " +
                   // 如果key已经存在，则判断filed是否相同，如果相同，则锁的重入次数加1，并重新设置过期时间 返回空值nil，表示获取锁成功
                  /**
                   *  "lockName":{
                   * 	     //客户端ID:重入次数加了1
                   * 	     "11bb52bc-a764-4649-8b46-a61513d7fe44:1":2
                   *      }
                   */
                  "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
                      "redis.call('hincrby', KEYS[1], ARGV[2], 1); " +
                      "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                      "return nil; " +
                  "end; " +
                   // 否则加锁失败，返回当前锁的失效时间
                  "return redis.call('pttl', KEYS[1]);",
                    Collections.<Object>singletonList(getName()), internalLockLeaseTime, getLockName(threadId));
    }
    
    private void acquireFailed(long threadId) {
        get(acquireFailedAsync(threadId));
    }
    
    protected RFuture<Void> acquireFailedAsync(long threadId) {
        return RedissonPromise.newSucceededFuture(null);
    }

    // 参数1 waitTime：向Redis获取锁的超时时间
    // 参数2 leaseTime：锁的失效时间(从开始获取锁时计时)
    // 参数3 unit：时间单位
    @Override
    public boolean tryLock(long waitTime, long leaseTime, TimeUnit unit) throws InterruptedException {

        // 获取锁的超时等待时间
        long time = unit.toMillis(waitTime);
        // 获取当前的时间戳
        long current = System.currentTimeMillis();
        //获取当前线程id
        long threadId = Thread.currentThread().getId();
        // 尝试获取锁并返回锁的剩余失效时间
        Long ttl = tryAcquire(leaseTime, unit, threadId);

        // 如果返回的失效时间为空，表示锁获取成功(后面看tryAcquire可以知道)
        if (ttl == null) {
            return true;
        }

        // 申请锁的耗时如果大于等于超时等待时间，则申请锁失败
        time -= System.currentTimeMillis() - current;
        if (time <= 0) {
            acquireFailed(threadId);
            return false;
        }
        
        current = System.currentTimeMillis();
        /**
         *    订阅锁释放事件，并通过await方法阻塞等待锁释放，有效的解决了无效的锁申请浪费资源的问题：
         *    基于信息量，当锁被其它线程占用时，当前线程通过 Redis 的 channel 订阅锁的释放事件，一旦锁释放会发消息通知等待的线程进行锁的竞争
         */
        RFuture<RedissonLockEntry> subscribeFuture = subscribe(threadId);
        /**
         *      await 方法内部是用CountDownLatch来实现阻塞，获取subscribe异步执行的结果
         *     当 subscribeFuture.await返回false，说明等待时间已经超出获取锁的超时等待时间
         */
        if (!subscribeFuture.await(time, TimeUnit.MILLISECONDS)) {
            if (!subscribeFuture.cancel(false)) {
                subscribeFuture.onComplete((res, e) -> {
                    if (e == null) {
                        // 取消解锁事件的订阅
                        unsubscribe(subscribeFuture, threadId);
                    }
                });
            }
            // 申请锁失败
            acquireFailed(threadId);
            return false;
        }

        // 当 subscribeFuture.await返回true，说明锁已经释放，进入循环尝试竞争锁
        try {
            // 计算获取锁的总耗时，如果大于等于最大等待时间，则获取锁失败
            time -= System.currentTimeMillis() - current;
            if (time <= 0) {
                acquireFailed(threadId);
                return false;
            }

            /**
             *      收到锁释放的信号后，在超时等待时间之内，循环一次接着一次的尝试获取锁
             *
             *      若在最大等待时间之内还没获取到锁，则认为获取锁失败，返回false结束循环
             */
            while (true) {
                long currentTime = System.currentTimeMillis();
                // 尝试获取锁
                ttl = tryAcquire(leaseTime, unit, threadId);
                // 获取锁成功,与上面一样
                if (ttl == null) {
                    return true;
                }
                // 计算获取锁的总耗时，如果大于等于最大等待时间，则获取锁失败
                time -= System.currentTimeMillis() - currentTime;
                if (time <= 0) {
                    acquireFailed(threadId);
                    return false;
                }


                // 阻塞等待锁（通过信号量(共享锁)阻塞,等待解锁消息）：
                currentTime = System.currentTimeMillis();
                if (ttl >= 0 && ttl < time) {
                    // 如果锁的剩余小于超时等待时间 ,就在 ttl 时间内，从Entry的信号量获取一个许可
                    getEntry(threadId).getLatch().tryAcquire(ttl, TimeUnit.MILLISECONDS);
                } else {
                    // 否则就在wait time 时间内从Entry的信号量获取一个许可
                    getEntry(threadId).getLatch().tryAcquire(time, TimeUnit.MILLISECONDS);
                }
                // 再次判断是否超时
                time -= System.currentTimeMillis() - currentTime;
                if (time <= 0) {
                    acquireFailed(threadId);
                    return false;
                }
            }
        } finally {
            // 最终不管是否加锁成功，都要取消解锁事件的订阅
            unsubscribe(subscribeFuture, threadId);
        }
//        return get(tryLockAsync(waitTime, leaseTime, unit));
    }

    protected RedissonLockEntry getEntry(long threadId) {
        return pubSub.getEntry(getEntryName());
    }

    protected RFuture<RedissonLockEntry> subscribe(long threadId) {
        return pubSub.subscribe(getEntryName(), getChannelName());
    }

    protected void unsubscribe(RFuture<RedissonLockEntry> future, long threadId) {
        pubSub.unsubscribe(future.getNow(), getEntryName(), getChannelName());
    }

    @Override
    public boolean tryLock(long waitTime, TimeUnit unit) throws InterruptedException {
        return tryLock(waitTime, -1, unit);
    }

    @Override
    public void unlock() {
        try {
            get(unlockAsync(Thread.currentThread().getId()));
        } catch (RedisException e) {
            if (e.getCause() instanceof IllegalMonitorStateException) {
                throw (IllegalMonitorStateException) e.getCause();
            } else {
                throw e;
            }
        }
        
//        Future<Void> future = unlockAsync();
//        future.awaitUninterruptibly();
//        if (future.isSuccess()) {
//            return;
//        }
//        if (future.cause() instanceof IllegalMonitorStateException) {
//            throw (IllegalMonitorStateException)future.cause();
//        }
//        throw commandExecutor.convertException(future);
    }

    @Override
    public Condition newCondition() {
        // TODO implement
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean forceUnlock() {
        return get(forceUnlockAsync());
    }

    @Override
    public RFuture<Boolean> forceUnlockAsync() {
        cancelExpirationRenewal(null);
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "if (redis.call('del', KEYS[1]) == 1) then "
                + "redis.call('publish', KEYS[2], ARGV[1]); "
                + "return 1 "
                + "else "
                + "return 0 "
                + "end",
                Arrays.<Object>asList(getName(), getChannelName()), LockPubSub.UNLOCK_MESSAGE);
    }

    @Override
    public boolean isLocked() {
        return isExists();
    }
    
    @Override
    public RFuture<Boolean> isLockedAsync() {
        return isExistsAsync();
    }

    @Override
    public RFuture<Boolean> isExistsAsync() {
        return commandExecutor.writeAsync(getName(), codec, RedisCommands.EXISTS, getName());
    }

    @Override
    public boolean isHeldByCurrentThread() {
        return isHeldByThread(Thread.currentThread().getId());
    }

    @Override
    public boolean isHeldByThread(long threadId) {
        RFuture<Boolean> future = commandExecutor.writeAsync(getName(), LongCodec.INSTANCE, RedisCommands.HEXISTS, getName(), getLockName(threadId));
        return get(future);
    }

    private static final RedisCommand<Integer> HGET = new RedisCommand<Integer>("HGET", ValueType.MAP_VALUE, new IntegerReplayConvertor(0));
    
    public RFuture<Integer> getHoldCountAsync() {
        return commandExecutor.writeAsync(getName(), LongCodec.INSTANCE, HGET, getName(), getLockName(Thread.currentThread().getId()));
    }
    
    @Override
    public int getHoldCount() {
        return get(getHoldCountAsync());
    }

    @Override
    public RFuture<Boolean> deleteAsync() {
        return forceUnlockAsync();
    }

    @Override
    public RFuture<Void> unlockAsync() {
        long threadId = Thread.currentThread().getId();
        return unlockAsync(threadId);
    }

    protected RFuture<Boolean> unlockInnerAsync(long threadId) {
        //执行lua脚本实现解锁
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                // KEYS[1]：是锁的key
                // KEYS[2]：redisson_lock__channel"+":{" + KEYS[1] + "}" || "redisson_lock__channel"+":" + KEYS[1]
                // ARGV[1]：LockPubSub.UNLOCK_MESSAGE表示解锁事件的消息，用于消息发布
                // ARGV[2]：internalLockLeaseTime锁的失效时间，用于重入锁的失效时间更新
                // ARGV[3]：当前线程获取分布式锁对应的key 即hash里面的field UUID+threadId

                // 如果key不存在或field不存在，解锁失败
                "if (redis.call('hexists', KEYS[1], ARGV[3]) == 0) then " +
                    "return nil;" +
                "end; " +
                 //锁的重入次数减1
                "local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1); " +
                 // 如果重入次数还大于0，说明该线程依然持有锁，更新失效时间
                "if (counter > 0) then " +
                    "redis.call('pexpire', KEYS[1], ARGV[2]); " +
                    "return 0; " +
                // 删除key以释放锁，并发布解锁的消息，通知其它线程来竞争锁
                "else " +
                    "redis.call('del', KEYS[1]); " +
                    "redis.call('publish', KEYS[2], ARGV[1]); " +
                    "return 1; "+
                "end; " +
                "return nil;",
                Arrays.<Object>asList(getName(), getChannelName()), LockPubSub.UNLOCK_MESSAGE, internalLockLeaseTime, getLockName(threadId));

    }
    
    @Override
    public RFuture<Void> unlockAsync(long threadId) {
        RPromise<Void> result = new RedissonPromise<Void>();
        //释放锁
        RFuture<Boolean> future = unlockInnerAsync(threadId);

        future.onComplete((opStatus, e) -> {
            if (e != null) {
                cancelExpirationRenewal(threadId);
                result.tryFailure(e);
                return;
            }
            //释放锁失败
            if (opStatus == null) {
                IllegalMonitorStateException cause = new IllegalMonitorStateException("attempt to unlock lock, not locked by current thread by node id: "
                        + id + " thread-id: " + threadId);
                result.tryFailure(cause);
                return;
            }

            //取消当前线程的锁续期
            cancelExpirationRenewal(threadId);
            result.trySuccess(null);
        });

        return result;
    }

    @Override
    public RFuture<Void> lockAsync() {
        return lockAsync(-1, null);
    }

    @Override
    public RFuture<Void> lockAsync(long leaseTime, TimeUnit unit) {
        long currentThreadId = Thread.currentThread().getId();
        return lockAsync(leaseTime, unit, currentThreadId);
    }

    @Override
    public RFuture<Void> lockAsync(long currentThreadId) {
        return lockAsync(-1, null, currentThreadId);
    }
    
    @Override
    public RFuture<Void> lockAsync(long leaseTime, TimeUnit unit, long currentThreadId) {
        RPromise<Void> result = new RedissonPromise<Void>();
        RFuture<Long> ttlFuture = tryAcquireAsync(leaseTime, unit, currentThreadId);
        ttlFuture.onComplete((ttl, e) -> {
            if (e != null) {
                result.tryFailure(e);
                return;
            }

            // lock acquired
            if (ttl == null) {
                if (!result.trySuccess(null)) {
                    unlockAsync(currentThreadId);
                }
                return;
            }

            RFuture<RedissonLockEntry> subscribeFuture = subscribe(currentThreadId);
            subscribeFuture.onComplete((res, ex) -> {
                if (ex != null) {
                    result.tryFailure(ex);
                    return;
                }

                lockAsync(leaseTime, unit, subscribeFuture, result, currentThreadId);
            });
        });

        return result;
    }

    private void lockAsync(long leaseTime, TimeUnit unit,
            RFuture<RedissonLockEntry> subscribeFuture, RPromise<Void> result, long currentThreadId) {
        RFuture<Long> ttlFuture = tryAcquireAsync(leaseTime, unit, currentThreadId);
        ttlFuture.onComplete((ttl, e) -> {
            if (e != null) {
                unsubscribe(subscribeFuture, currentThreadId);
                result.tryFailure(e);
                return;
            }

            // lock acquired
            if (ttl == null) {
                unsubscribe(subscribeFuture, currentThreadId);
                if (!result.trySuccess(null)) {
                    unlockAsync(currentThreadId);
                }
                return;
            }

            RedissonLockEntry entry = getEntry(currentThreadId);
            if (entry.getLatch().tryAcquire()) {
                lockAsync(leaseTime, unit, subscribeFuture, result, currentThreadId);
            } else {
                // waiting for message
                AtomicReference<Timeout> futureRef = new AtomicReference<Timeout>();
                Runnable listener = () -> {
                    if (futureRef.get() != null) {
                        futureRef.get().cancel();
                    }
                    lockAsync(leaseTime, unit, subscribeFuture, result, currentThreadId);
                };

                entry.addListener(listener);

                if (ttl >= 0) {
                    Timeout scheduledFuture = commandExecutor.getConnectionManager().newTimeout(new TimerTask() {
                        @Override
                        public void run(Timeout timeout) throws Exception {
                            if (entry.removeListener(listener)) {
                                lockAsync(leaseTime, unit, subscribeFuture, result, currentThreadId);
                            }
                        }
                    }, ttl, TimeUnit.MILLISECONDS);
                    futureRef.set(scheduledFuture);
                }
            }
        });
    }

    @Override
    public RFuture<Boolean> tryLockAsync() {
        return tryLockAsync(Thread.currentThread().getId());
    }

    @Override
    public RFuture<Boolean> tryLockAsync(long threadId) {
        return tryAcquireOnceAsync(-1, null, threadId);
    }

    @Override
    public RFuture<Boolean> tryLockAsync(long waitTime, TimeUnit unit) {
        return tryLockAsync(waitTime, -1, unit);
    }

    @Override
    public RFuture<Boolean> tryLockAsync(long waitTime, long leaseTime, TimeUnit unit) {
        long currentThreadId = Thread.currentThread().getId();
        return tryLockAsync(waitTime, leaseTime, unit, currentThreadId);
    }

    @Override
    public RFuture<Boolean> tryLockAsync(long waitTime, long leaseTime, TimeUnit unit,
            long currentThreadId) {
        RPromise<Boolean> result = new RedissonPromise<Boolean>();

        AtomicLong time = new AtomicLong(unit.toMillis(waitTime));
        long currentTime = System.currentTimeMillis();
        RFuture<Long> ttlFuture = tryAcquireAsync(leaseTime, unit, currentThreadId);
        ttlFuture.onComplete((ttl, e) -> {
            if (e != null) {
                result.tryFailure(e);
                return;
            }

            // lock acquired
            if (ttl == null) {
                if (!result.trySuccess(true)) {
                    unlockAsync(currentThreadId);
                }
                return;
            }

            long el = System.currentTimeMillis() - currentTime;
            time.addAndGet(-el);
            
            if (time.get() <= 0) {
                trySuccessFalse(currentThreadId, result);
                return;
            }
            
            long current = System.currentTimeMillis();
            AtomicReference<Timeout> futureRef = new AtomicReference<Timeout>();
            RFuture<RedissonLockEntry> subscribeFuture = subscribe(currentThreadId);
            subscribeFuture.onComplete((r, ex) -> {
                if (ex != null) {
                    result.tryFailure(ex);
                    return;
                }

                if (futureRef.get() != null) {
                    futureRef.get().cancel();
                }

                long elapsed = System.currentTimeMillis() - current;
                time.addAndGet(-elapsed);
                
                tryLockAsync(time, leaseTime, unit, subscribeFuture, result, currentThreadId);
            });
            if (!subscribeFuture.isDone()) {
                Timeout scheduledFuture = commandExecutor.getConnectionManager().newTimeout(new TimerTask() {
                    @Override
                    public void run(Timeout timeout) throws Exception {
                        if (!subscribeFuture.isDone()) {
                            subscribeFuture.cancel(false);
                            trySuccessFalse(currentThreadId, result);
                        }
                    }
                }, time.get(), TimeUnit.MILLISECONDS);
                futureRef.set(scheduledFuture);
            }
        });


        return result;
    }

    private void trySuccessFalse(long currentThreadId, RPromise<Boolean> result) {
        acquireFailedAsync(currentThreadId).onComplete((res, e) -> {
            if (e == null) {
                result.trySuccess(false);
            } else {
                result.tryFailure(e);
            }
        });
    }

    private void tryLockAsync(AtomicLong time, long leaseTime, TimeUnit unit,
            RFuture<RedissonLockEntry> subscribeFuture, RPromise<Boolean> result, long currentThreadId) {
        if (result.isDone()) {
            unsubscribe(subscribeFuture, currentThreadId);
            return;
        }
        
        if (time.get() <= 0) {
            unsubscribe(subscribeFuture, currentThreadId);
            trySuccessFalse(currentThreadId, result);
            return;
        }
        
        long curr = System.currentTimeMillis();
        RFuture<Long> ttlFuture = tryAcquireAsync(leaseTime, unit, currentThreadId);
        ttlFuture.onComplete((ttl, e) -> {
                if (e != null) {
                    unsubscribe(subscribeFuture, currentThreadId);
                    result.tryFailure(e);
                    return;
                }

                // lock acquired
                if (ttl == null) {
                    unsubscribe(subscribeFuture, currentThreadId);
                    if (!result.trySuccess(true)) {
                        unlockAsync(currentThreadId);
                    }
                    return;
                }
                
                long el = System.currentTimeMillis() - curr;
                time.addAndGet(-el);
                
                if (time.get() <= 0) {
                    unsubscribe(subscribeFuture, currentThreadId);
                    trySuccessFalse(currentThreadId, result);
                    return;
                }

                // waiting for message
                long current = System.currentTimeMillis();
                RedissonLockEntry entry = getEntry(currentThreadId);
                if (entry.getLatch().tryAcquire()) {
                    tryLockAsync(time, leaseTime, unit, subscribeFuture, result, currentThreadId);
                } else {
                    AtomicBoolean executed = new AtomicBoolean();
                    AtomicReference<Timeout> futureRef = new AtomicReference<Timeout>();
                    Runnable listener = () -> {
                        executed.set(true);
                        if (futureRef.get() != null) {
                            futureRef.get().cancel();
                        }

                        long elapsed = System.currentTimeMillis() - current;
                        time.addAndGet(-elapsed);
                        
                        tryLockAsync(time, leaseTime, unit, subscribeFuture, result, currentThreadId);
                    };
                    entry.addListener(listener);

                    long t = time.get();
                    if (ttl >= 0 && ttl < time.get()) {
                        t = ttl;
                    }
                    if (!executed.get()) {
                        Timeout scheduledFuture = commandExecutor.getConnectionManager().newTimeout(new TimerTask() {
                            @Override
                            public void run(Timeout timeout) throws Exception {
                                if (entry.removeListener(listener)) {
                                    long elapsed = System.currentTimeMillis() - current;
                                    time.addAndGet(-elapsed);
                                    
                                    tryLockAsync(time, leaseTime, unit, subscribeFuture, result, currentThreadId);
                                }
                            }
                        }, t, TimeUnit.MILLISECONDS);
                        futureRef.set(scheduledFuture);
                    }
                }
        });
    }


}
