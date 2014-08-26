package com.hazelcast.config;
/**
 * Eviction Policy enum.
 */
public enum EvictionPolicy {
    /**
     * Least Recently Used
     */
    LRU,
    /**
     * Least Frequently Used
     */
    LFU,
    /**
     * None
     */
    NONE,
    /**
     * Randomly
     */
    RANDOM
}
