package com.hazelcast.internal.dynamicconfig;

/**
 * Behaviour when detects a configuration conflict while registering a new dynamic configuraiton.
 *
 */
public enum ConfigCheckMode {
    /**
     * Throw {@link com.hazelcast.config.ConfigurationException} when a configuration conflict is detected
     *
     */
    THROW_EXCEPTION,

    /**
     * Log a warning when detect a configuration conflict
     *
     */
    WARNING,

    /**
     * Ignore configuration conflicts.
     * The caller can still decide to a log it, but it should not go into WARNING level, but
     * into FINEST/DEBUG, etc..
     *
     */
    SILENT
}
