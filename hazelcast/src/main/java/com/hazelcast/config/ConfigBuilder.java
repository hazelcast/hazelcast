package com.hazelcast.config;

/**
 * Interface for all config builders.
 */

public interface ConfigBuilder {

    /**
     * Builds Config object.
     *
     * @returns Config object
     */
    Config build();
}
