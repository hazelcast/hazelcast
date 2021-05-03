package com.hazelcast.config;

import com.hazelcast.spi.properties.HazelcastProperty;

import static com.hazelcast.instance.BuildInfoProvider.HAZELCAST_INTERNAL_OVERRIDE_VERSION;

public class AbstractConfigBuilder {
    
    /**
     * Set it to {@code false} to skip the configuration validation.
     */
    protected final HazelcastProperty VALIDATION_ENABLED_PROP = new HazelcastProperty("hazelcast.config.schema.validation.enabled", "true");
    
    protected final boolean shouldValidateTheSchema() {
        return !"false".equals(VALIDATION_ENABLED_PROP.getSystemProperty())
                && System.getProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION) == null;
    }


}
