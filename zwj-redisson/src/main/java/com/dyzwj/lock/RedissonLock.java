package com.dyzwj.lock;

import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.util.concurrent.TimeUnit;

public class RedissonLock {

    public static void main(String[] args) throws Exception {
        //构建一个配置信息对象
        Config config = new Config();
        config.useClusterServers()
                //定时扫描连接信息 默认1000ms
                .setScanInterval(2000)
                .addNodeAddress("redis://127.0.0.1:7001");
        //因为Redisson 是基于redis封装的一套便于复杂操作的框架
        //所以这里构建对象肯定是创建一些与redis的连接
        RedissonClient redisson = Redisson.create(config);
        //这里是重点 获取锁，这也是重点分析的地方
        RLock lock = redisson.getLock("lock");
        //加锁
        lock.lock();

        // 加锁以后10秒钟自动解锁
        // 无需调用unlock方法手动解锁
        lock.lock(10, TimeUnit.SECONDS);

        // 尝试加锁，最多等待100秒，上锁以后10秒自动解锁
        boolean res = lock.tryLock(100, 10, TimeUnit.SECONDS);


        //释放锁
        lock.unlock();
    }
}
