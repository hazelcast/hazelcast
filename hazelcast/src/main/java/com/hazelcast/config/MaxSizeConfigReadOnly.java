package com.hazelcast.config;

/**
 * @ali 10/11/13
 */
public class MaxSizeConfigReadOnly extends MaxSizeConfig {

    public MaxSizeConfigReadOnly(MaxSizeConfig config) {
        this.setSize(config.getSize());
        this.setMaxSizePolicy(config.getMaxSizePolicy());
    }

    public MaxSizeConfig setSize(int size) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    public MaxSizeConfig setMaxSizePolicy(MaxSizePolicy maxSizePolicy) {
        throw new UnsupportedOperationException("This config is read-only");
    }
}
