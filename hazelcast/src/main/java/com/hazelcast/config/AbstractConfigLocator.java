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

    public boolean isConfigPresent() {
        return in != null || configurationFile != null || configurationUrl != null;
    }

    /**
     * Locates the configuration file in a system property
     *
     * @return true if the configuration file is found in the system property
     * @throws HazelcastException if there was a problem locating the
     *                            configuration file
     */
    public abstract boolean locateFromSystemProperty();

    /**
     * Locates the configuration file in the working directory
     *
     * @return true if the configuration file is found in the working directory
     * @throws HazelcastException if there was a problem locating the
     *                            configuration file
     */
    protected abstract boolean locateInWorkDir();

    /**
     * Locates the configuration file on the classpath
     *
     * @return true if the configuration file is found on the classpath
     * @throws HazelcastException if there was a problem locating the
     *                            configuration file
     */
    protected abstract boolean locateOnClasspath();

    /**
     * Locates the default configuration file
     *
     * @return true always, indicating the default configuration file is
     * found
     * @throws HazelcastException if there was a problem locating the
     *                            default configuration file
     */
    public abstract boolean locateDefault();

    public boolean locateEveryWhere() {
        return locateFromSystemProperty()
                || locateInWorkDir()
                || locateOnClasspath()
                || locateDefault();
    }

    public boolean locateInWorkDirOrOnClasspath() {
        return locateInWorkDir() || locateOnClasspath();
    }

    protected void loadDefaultConfigurationFromClasspath(String defaultConfigFile) {
        try {
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
        } catch (RuntimeException e) {
            throw new HazelcastException(e);
        }
    }

    protected boolean loadConfigurationFromClasspath(String configFileName) {
        try {
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
        } catch (RuntimeException e) {
            throw new HazelcastException(e);
        }
    }

    protected boolean loadFromWorkingDirectory(String configFilePath) {
        try {
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
        } catch (RuntimeException e) {
            throw new HazelcastException(e);
        }
    }

    protected boolean loadFromSystemProperty(String propertyKey, String... expectedExtensions) {
        try {
            String configSystemProperty = System.getProperty(propertyKey);

            if (configSystemProperty == null) {
                LOGGER.finest("Could not find '" + propertyKey + "' System property");
                return false;
            }

            if (expectedExtensions != null && expectedExtensions.length > 0
                    && !isExpectedExtensionConfigured(configSystemProperty, expectedExtensions)) {
                return false;
            }

            LOGGER.info("Loading configuration " + configSystemProperty + " from System property 'hazelcast.config'");

            if (configSystemProperty.startsWith("classpath:")) {
                loadSystemPropertyClassPathResource(configSystemProperty);
            } else {
                loadSystemPropertyFileResource(configSystemProperty);
            }
            return true;
        } catch (RuntimeException e) {
            throw new HazelcastException(e);
        }
    }

    private boolean isExpectedExtensionConfigured(String configSystemProperty, String[] expectedExtensions) {
        boolean expectedExtension = false;
        String configSystemPropertyLower = configSystemProperty.toLowerCase();
        for (String extension : expectedExtensions) {
            if (configSystemPropertyLower.endsWith("." + extension.toLowerCase())) {
                expectedExtension = true;
                break;
            }
        }
        return expectedExtension;
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
