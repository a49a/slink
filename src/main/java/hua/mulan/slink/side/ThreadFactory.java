package hua.mulan.slink.side;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @program: slink
 * @author: wuren
 * @create: 2020/08/24
 **/
public class ThreadFactory implements java.util.concurrent.ThreadFactory {
    private final static AtomicInteger POOL_NUMBER = new AtomicInteger(1);
    private final static AtomicInteger THREAD_NUMBER = new AtomicInteger(1);
    private final ThreadGroup group;
    private final String namePrefix;

    public ThreadFactory(String factoryName) {
        SecurityManager s = System.getSecurityManager();
        group = (s != null) ? s.getThreadGroup() :
            Thread.currentThread().getThreadGroup();
        namePrefix = factoryName + "-pool-" +
            POOL_NUMBER.getAndIncrement() +
            "-thread-";
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(group, r,
            namePrefix + THREAD_NUMBER.getAndIncrement(),
            0);
        if (t.isDaemon()) {
            t.setDaemon(false);
        }
        if (t.getPriority() != Thread.NORM_PRIORITY) {
            t.setPriority(Thread.NORM_PRIORITY);
        }
        return t;
    }
}
