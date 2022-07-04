/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.config;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Properties;

import static com.hazelcast.internal.util.Preconditions.checkTrue;
import static com.hazelcast.internal.util.StringUtil.isNullOrEmptyAfterTrim;
import static com.hazelcast.internal.util.StringUtil.stringToBytes;

/**
 * Creates a {@link Config} loaded from an in-memory Hazelcast YAML String.
 * <p>
 * Unlike {@link Config#loadFromString(String)} and its variants, a configuration constructed via
 * {@code InMemoryYamlConfig} does not apply overrides found in environment variables/system properties.
 */
public class InMemoryYamlConfig extends Config {
    private static final ILogger LOGGER = Logger.getLogger(InMemoryYamlConfig.class);

    /**
     * Creates a Config from the provided YAML string and uses the System.properties to resolve variables
     * in the YAML.
     *
     * @param yaml the YAML content as a Hazelcast YAML String
     * @throws IllegalArgumentException      if the YAML is null or empty
     * @throws InvalidConfigurationException if the YAML content is invalid
     */
    public InMemoryYamlConfig(String yaml) {
        this(yaml, System.getProperties());
    }

    /**
     * Creates a Config from the provided YAML string and properties to resolve the variables in the YAML.
     *
     * @param yaml the YAML content as a Hazelcast YAML String
     * @throws IllegalArgumentException      if the YAML is null or empty or if properties is null
     * @throws InvalidConfigurationException if the YAML content is invalid
     */
    public InMemoryYamlConfig(String yaml, Properties properties) {
        LOGGER.info("Configuring Hazelcast from 'in-memory YAML'.");
        if (isNullOrEmptyAfterTrim(yaml)) {
            throw new IllegalArgumentException("YAML configuration is null or empty! Please use a well-structured YAML.");
        }
        checkTrue(properties != null, "properties can't be null");

        InputStream in = new ByteArrayInputStream(stringToBytes(yaml));
        new YamlConfigBuilder(in).setProperties(properties).build(this);
    }
}
