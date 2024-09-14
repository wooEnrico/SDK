package io.github.wooenrico.kafka;

import java.io.Serializable;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedThreadFactory implements ThreadFactory, Serializable {

    private final AtomicInteger threadCount = new AtomicInteger();
    private final ThreadGroup threadGroup;
    private final String threadName;

    private boolean daemon = false;
    private int threadPriority = 5;

    public NamedThreadFactory(String threadName) {
        this.threadGroup = new ThreadGroup(threadName + "-group");
        this.threadName = threadName;
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

    public void setDaemon(boolean daemon) {
        this.daemon = daemon;
    }

    public void setThreadPriority(int threadPriority) {
        this.threadPriority = threadPriority;
    }

    public Thread createThread(Runnable runnable) {
        Thread thread = new Thread(this.threadGroup, runnable, this.nextThreadName());
        thread.setPriority(this.threadPriority);
        thread.setDaemon(this.daemon);
        return thread;
    }

    protected String nextThreadName() {
        return this.threadName + "-" + this.threadCount.incrementAndGet();
    }

    public Thread newThread(Runnable runnable) {
        return this.createThread(runnable);
    }
}
