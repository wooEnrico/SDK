package io.github.wooenrico.kafka;

import java.io.Serializable;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedThreadFactory implements ThreadFactory, Serializable {

    private final AtomicInteger threadCount = new AtomicInteger();
    private final ThreadGroup threadGroup;
    private final String threadName;
    private final boolean daemon;
    private final int threadPriority;

    public NamedThreadFactory(String threadName) {
        this(threadName, false);
    }

    public NamedThreadFactory(String threadName, boolean daemon) {
        this(threadName, daemon, Thread.NORM_PRIORITY);
    }

    public NamedThreadFactory(String threadName, boolean daemon, int threadPriority) {
        this.threadGroup = new ThreadGroup(threadName);
        this.threadName = threadName;
        this.daemon = daemon;
        this.threadPriority = threadPriority;
    }

    public ThreadGroup getThreadGroup() {
        return threadGroup;
    }

    public String getThreadName() {
        return threadName;
    }

    public boolean isDaemon() {
        return daemon;
    }

    public int getThreadPriority() {
        return threadPriority;
    }

    protected String nextThreadName() {
        return this.threadName + "-" + this.threadCount.incrementAndGet();
    }

    public Thread newThread(Runnable runnable) {
        Thread thread = new Thread(this.threadGroup, runnable, this.nextThreadName());
        thread.setPriority(this.threadPriority);
        thread.setDaemon(this.daemon);
        return thread;
    }
}
