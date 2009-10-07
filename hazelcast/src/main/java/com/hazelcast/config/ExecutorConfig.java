/**
 *
 */
package com.hazelcast.config;

public final class ExecutorConfig {

    public final static int DEFAULT_CORE_POOL_SIZE = 10;
    public final static int DEFAULT_MAX_POOL_SIZE = 50;
    public final static int DEFAULT_KEEPALIVE_SECONDS = 60;

    private int corePoolSize = DEFAULT_CORE_POOL_SIZE;

    private int maxPoolsize = DEFAULT_MAX_POOL_SIZE;

    private int keepAliveSeconds = DEFAULT_KEEPALIVE_SECONDS;

    /**
     * @return the corePoolSize
     */
    public int getCorePoolSize() {
        return corePoolSize;
    }

    /**
     * @param corePoolSize the corePoolSize to set
     */
    public void setCorePoolSize(int corePoolSize) {
        this.corePoolSize = corePoolSize;
    }

    /**
     * @return the maxPoolsize
     */
    public int getMaxPoolsize() {
        return maxPoolsize;
    }

    /**
     * @param maxPoolsize the maxPoolsize to set
     */
    public void setMaxPoolsize(int maxPoolsize) {
        this.maxPoolsize = maxPoolsize;
    }

    /**
     * @return the keepAliveSeconds
     */
    public int getKeepAliveSeconds() {
        return keepAliveSeconds;
    }

    /**
     * @param keepAliveSeconds the keepAliveSeconds to set
     */
    public void setKeepAliveSeconds(int keepAliveSeconds) {
        this.keepAliveSeconds = keepAliveSeconds;
    }
}
