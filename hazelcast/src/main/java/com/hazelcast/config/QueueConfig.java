/**
 *
 */
package com.hazelcast.config;

public final class QueueConfig {

    public final static int DEFAULT_MAX_SIZE_PER_JVM = Integer.MAX_VALUE;
    public final static int DEFAULT_TTL_SECONDS = Integer.MAX_VALUE;

    private String name;

    private int maxSizePerJVM = DEFAULT_MAX_SIZE_PER_JVM;

    private int timeToLiveSeconds = DEFAULT_TTL_SECONDS;

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return the maxSizePerJVM
     */
    public int getMaxSizePerJVM() {
        return maxSizePerJVM;
    }

    /**
     * @param maxSizePerJVM the maxSizePerJVM to set
     */
    public void setMaxSizePerJVM(int maxSizePerJVM) {
        this.maxSizePerJVM = maxSizePerJVM;
    }

    /**
     * @return the timeToLiveSeconds
     */
    public int getTimeToLiveSeconds() {
        return timeToLiveSeconds;
    }

    /**
     * @param timeToLiveSeconds the timeToLiveSeconds to set
     */
    public void setTimeToLiveSeconds(int timeToLiveSeconds) {
        this.timeToLiveSeconds = timeToLiveSeconds;
    }
}