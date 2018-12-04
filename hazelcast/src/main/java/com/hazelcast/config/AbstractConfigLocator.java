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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Abstract base class for config locators
 *
 * @see XmlConfigLocator
 * @see YamlConfigLocator
 */
public abstract class AbstractConfigLocator {
    private static final ILogger LOGGER = Logger.getLogger(AbstractConfigLocator.class);
    private InputStream in;
    private File configurationFile;
    private URL configurationUrl;

    public InputStream getIn() {
        return in;
    }

    public File getConfigurationFile() {
        return configurationFile;
    }

    public URL getConfigurationUrl() {
        return configurationUrl;
    }

    protected void loadDefaultConfigurationFromClasspath(String defaultConfigFile) {
        LOGGER.info("Loading '" + defaultConfigFile + "' from classpath.");

        configurationUrl = Config.class.getClassLoader().getResource(defaultConfigFile);

        if (configurationUrl == null) {
            throw new HazelcastException("Could not find '" + defaultConfigFile + "' in the classpath!"
                    + "This may be due to a wrong-packaged or corrupted jar file.");
        }

        in = Config.class.getClassLoader().getResourceAsStream(defaultConfigFile);
        if (in == null) {
            throw new HazelcastException("Could not load '" + defaultConfigFile + "' from classpath");
        }
    }

    protected boolean loadHazelcastConfigFromClasspath(String configFileName) {
        URL url = Config.class.getClassLoader().getResource(configFileName);
        if (url == null) {
            LOGGER.finest("Could not find '" + configFileName + "' in classpath.");
            return false;
        }

        LOGGER.info("Loading '" + configFileName + "' from classpath.");

        configurationUrl = url;
        in = Config.class.getClassLoader().getResourceAsStream(configFileName);
        if (in == null) {
            throw new HazelcastException("Could not load '" + configFileName + "' from classpath");
        }
        return true;
    }

    protected boolean loadFromWorkingDirectory(String configFilePath) {
        File file = new File(configFilePath);
        if (!file.exists()) {
            LOGGER.finest("Could not find '" + configFilePath + "' in working directory.");
            return false;
        }

        LOGGER.info("Loading '" + configFilePath + "' from working directory.");

        configurationFile = file;
        try {
            in = new FileInputStream(file);
        } catch (FileNotFoundException e) {
            throw new HazelcastException("Failed to open file: " + file.getAbsolutePath(), e);
        }
        return true;
    }

    protected boolean loadFromSystemProperty(String propertyKey) {
        String configSystemProperty = System.getProperty(propertyKey);

        if (configSystemProperty == null) {
            LOGGER.finest("Could not 'hazelcast.config' System property");
            return false;
        }

        LOGGER.info("Loading configuration " + configSystemProperty + " from System property 'hazelcast.config'");

        if (configSystemProperty.startsWith("classpath:")) {
            loadSystemPropertyClassPathResource(configSystemProperty);
        } else {
            loadSystemPropertyFileResource(configSystemProperty);
        }
        return true;
    }

    private void loadSystemPropertyFileResource(String configSystemProperty) {
        // it's a file
        configurationFile = new File(configSystemProperty);
        LOGGER.info("Using configuration file at " + configurationFile.getAbsolutePath());

        if (!configurationFile.exists()) {
            String msg = "Config file at '" + configurationFile.getAbsolutePath() + "' doesn't exist.";
            throw new HazelcastException(msg);
        }

        try {
            in = new FileInputStream(configurationFile);
        } catch (FileNotFoundException e) {
            throw new HazelcastException("Failed to open file: " + configurationFile.getAbsolutePath(), e);
        }

        try {
            configurationUrl = configurationFile.toURI().toURL();
        } catch (MalformedURLException e) {
            throw new HazelcastException("Failed to create URL from the file: " + configurationFile.getAbsolutePath(), e);
        }
    }

    private void loadSystemPropertyClassPathResource(String configSystemProperty) {
        // it's an explicit configured classpath resource
        String resource = configSystemProperty.substring("classpath:".length());

        LOGGER.info("Using classpath resource at " + resource);

        if (resource.isEmpty()) {
            throw new HazelcastException("classpath resource can't be empty");
        }

        in = Config.class.getClassLoader().getResourceAsStream(resource);
        if (in == null) {
            throw new HazelcastException("Could not load classpath resource: " + resource);
        }
        configurationUrl = Config.class.getResource(resource);
    }
}
