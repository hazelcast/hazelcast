/**
 *
 */
package com.hazelcast.config;

public class ExecutorConfig {

    public final static int DEFAULT_CORE_POOL_SIZE = 10;
    public final static int DEFAULT_MAX_POOL_SIZE = 50;
    public final static int DEFAULT_KEEPALIVE_SECONDS = 60;

    private int corePoolSize = DEFAULT_CORE_POOL_SIZE;

    private int maxPoolSize = DEFAULT_MAX_POOL_SIZE;

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
    	if(corePoolSize > 0) {
            this.corePoolSize = corePoolSize;
    	} else {
    		throw new UnsupportedOperationException("corePoolSize must be positive");
    	}
    }

    /**
     * @return the maxPoolsize
     */
    public int getMaxPoolsize() {
        return maxPoolSize;
    }

    /**
     * @param maxPoolsize the maxPoolsize to set
     */
    public void setMaxPoolsize(final int maxPoolSize) {
    	if(maxPoolSize > 0) {
            this.maxPoolSize = maxPoolSize;
    	} else {
    		throw new UnsupportedOperationException("maxPoolSize must be positive");
    	}
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
    	if(keepAliveSeconds > 0) {
    	       this.keepAliveSeconds = keepAliveSeconds;
    	} else {
    		throw new UnsupportedOperationException("keepAlice seconds must be positive");
    	}
    }
}
