/**
 * 
 */
package com.hazelcast.config;

public class MapConfig {
    public String name;

    public int backupCount = 1;

    public int evictionPercentage = 25;

    public int timeToLiveSeconds = 0;

    public int maxSize = Integer.MAX_VALUE;

    public String evictionPolicy = "NONE";
}