package com.dyzwj.lock;

import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

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
        //释放锁
        lock.unlock();
    }
}
