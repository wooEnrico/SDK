package io.github.wooernico.kafka;

public class ExecutorConf {
    private int corePoolSize = Runtime.getRuntime().availableProcessors();
    private int maximumPoolSize = Runtime.getRuntime().availableProcessors() * 2 + 1;
    private long keepAliveTimeMill = 60000;
    private int queueSize = 100;

    public int getCorePoolSize() {
        return corePoolSize;
    }

    public void setCorePoolSize(int corePoolSize) {
        this.corePoolSize = corePoolSize;
    }

    public int getMaximumPoolSize() {
        return maximumPoolSize;
    }

    public void setMaximumPoolSize(int maximumPoolSize) {
        this.maximumPoolSize = maximumPoolSize;
    }

    public long getKeepAliveTimeMill() {
        return keepAliveTimeMill;
    }

    public void setKeepAliveTimeMill(long keepAliveTimeMill) {
        this.keepAliveTimeMill = keepAliveTimeMill;
    }

    public int getQueueSize() {
        return queueSize;
    }

    public void setQueueSize(int queueSize) {
        this.queueSize = queueSize;
    }

    @Override
    public String toString() {
        return "ExecutorConf{" +
                "corePoolSize=" + corePoolSize +
                ", maximumPoolSize=" + maximumPoolSize +
                ", keepAliveTimeMill=" + keepAliveTimeMill +
                ", queueSize=" + queueSize +
                '}';
    }
}

