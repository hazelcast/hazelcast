/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Properties;

/**
 * A {@link Config} which includes functionality for loading itself from a
 * YAML configuration file.
 */
public class FileSystemYamlConfig extends Config {

    private static final ILogger LOGGER = Logger.getLogger(FileSystemYamlConfig.class);

    /**
     * Creates a Config based on a Hazelcast yaml file and uses the System.properties to resolve
     * variables in the YAML.
     *
     * @param configFilename the path of the Hazelcast yaml configuration file
     * @throws NullPointerException                  if configFilename is {@code null}
     * @throws FileNotFoundException                 fi the file is not found
     * @throws com.hazelcast.core.HazelcastException if the YAML content is invalid
     */
    public FileSystemYamlConfig(String configFilename) throws FileNotFoundException {
        this(configFilename, System.getProperties());
    }

    /**
     * Creates a Config based on a Hazelcast YAML file.
     *
     * @param configFilename the path of the Hazelcast YAML configuration file
     * @param properties     the Properties to resolve variables in the YAML
     * @throws FileNotFoundException                 fi the file is not found
     * @throws NullPointerException                  if configFilename is {@code null}
     * @throws IllegalArgumentException              if properties is {@code null}
     * @throws com.hazelcast.core.HazelcastException if the YAML content is invalid
     */
    public FileSystemYamlConfig(String configFilename, Properties properties) throws FileNotFoundException {
        this(new File(configFilename), properties);
    }

    /**
     * Creates a Config based on a Hazelcast yaml file and uses the System.properties to resolve
     * variables in the YAML.
     *
     * @param configFile the path of the Hazelcast YAML configuration file
     * @throws FileNotFoundException                 if the file doesn't exist
     * @throws com.hazelcast.core.HazelcastException if the YAML content is invalid
     */
    public FileSystemYamlConfig(File configFile) throws FileNotFoundException {
        this(configFile, System.getProperties());
    }

    /**
     * Creates a Config based on a Hazelcast YAML file.
     *
     * @param configFile the path of the Hazelcast yaml configuration file
     * @param properties the Properties to resolve variables in the YAML
     * @throws IllegalArgumentException              if configFile or properties is {@code null}
     * @throws FileNotFoundException                 if the file doesn't exist
     * @throws com.hazelcast.core.HazelcastException if the YAML content is invalid
     */
    public FileSystemYamlConfig(File configFile, Properties properties) throws FileNotFoundException {
        if (configFile == null) {
            throw new IllegalArgumentException("configFile can't be null");
        }
        if (properties == null) {
            throw new IllegalArgumentException("properties can't be null");
        }

        LOGGER.info("Configuring Hazelcast from '" + configFile.getAbsolutePath() + "'.");
        InputStream in = new FileInputStream(configFile);
        new YamlConfigBuilder(in).setProperties(properties).build(this);
    }
}
