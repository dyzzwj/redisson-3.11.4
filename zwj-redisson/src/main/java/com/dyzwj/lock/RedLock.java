package com.dyzwj.lock;

import org.redisson.Redisson;
import org.redisson.RedissonMultiLock;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class RedLock {

    static Logger logger = LoggerFactory.getLogger(RedLock.class);

    public void main() {
        String key = "redlock";
        AtomicInteger atomicInteger = new AtomicInteger();

        // 获取三个完全独立的Redis实例的连接
        Config config1 = new Config();
        config1.useSingleServer().setAddress("redis://127.0.0.1:6379").setPassword("redis666").setDatabase(0);
        RedissonClient client1 = Redisson.create(config1);

        Config config2 = new Config();
        config2.useSingleServer().setAddress("redis://127.0.0.1:6378").setPassword("redis666").setDatabase(0);
        RedissonClient client2 = Redisson.create(config2);

        Config config3 = new Config();
        config3.useSingleServer().setAddress("redis://127.0.0.1:6377").setPassword("redis666").setDatabase(0);
        RedissonClient client3 = Redisson.create(config3);

        // 通过相同的Key分别获取三个实例的锁对象
        RLock rLock1 = client1.getLock(key);
        RLock rLock2 = client2.getLock(key);
        RLock rLock3 = client3.getLock(key);

        // 通过获取的三个锁对象来构建一个RedissonRedlock实例
        // 这里就是redlock和普通lock的区别
        RedissonMultiLock redlock = new RedissonMultiLock(rLock1, rLock2, rLock3);

        // 下面业务处理逻辑和锁的使用都与普通lock一致
        ExecutorService service = Executors.newFixedThreadPool(5);
        for (int i = 0; i < 5; ++i) {
            service.submit(() -> {
                boolean isLock;
                try {
                    // 参数1 waitTime：向Redis获取锁的超时时间
                    // 参数2 leaseTime：锁的失效时间(从开始获取锁时计时)
                    // 参数3 unit：时间单位
                    isLock = redlock.tryLock(50, 10000, TimeUnit.MILLISECONDS);
                    if (isLock) {
                        logger.info("我获取到锁啦");
                        atomicInteger.getAndIncrement();
                        logger.info("count: [{}]", atomicInteger);
                    }
                }catch (InterruptedException e){
                    e.printStackTrace();
                }finally {
                    // 最终释放锁
                    redlock.unlock();
                }
            });
        }
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
