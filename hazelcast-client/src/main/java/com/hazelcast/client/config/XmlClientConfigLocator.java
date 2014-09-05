package com.hazelcast.client.config;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URL;

/**
 * A support class for the {@link com.hazelcast.client.config.XmlClientConfigBuilder} to locate the client
 * xml configuration.
 */
public class XmlClientConfigLocator {
    private static final ILogger LOGGER = Logger.getLogger(XmlClientConfigLocator.class);

    private InputStream in;

    /**
     * Constructs a XmlClientConfigBuilder.
     *
     * @throws com.hazelcast.core.HazelcastException if the client XML config is not located.
     */
    public XmlClientConfigLocator() {
        try {
            if (loadFromSystemProperty()) {
                return;
            }

            if (loadFromWorkingDirectory()) {
                return;
            }

            if (loadClientHazelcastXmlFromClasspath()) {
                return;
            }

            loadDefaultConfigurationFromClasspath();
        } catch (final RuntimeException e) {
            throw new HazelcastException("Failed to load ClientConfig", e);
        }
    }

    public InputStream getIn() {
        return in;
    }

    private void loadDefaultConfigurationFromClasspath() {
        LOGGER.info("Loading 'hazelcast-client-default.xml' from classpath.");

        in = Config.class.getClassLoader().getResourceAsStream("hazelcast-client-default.xml");
        if (in == null) {
            throw new HazelcastException("Could not load 'hazelcast-client-default.xml' from classpath");
        }
    }

    private boolean loadClientHazelcastXmlFromClasspath() {
        URL url = Config.class.getClassLoader().getResource("hazelcast-client.xml");
        if (url == null) {
            LOGGER.finest("Could not find 'hazelcast-client.xml' in classpath.");
            return false;
        }

        LOGGER.info("Loading 'hazelcast-client.xml' from classpath.");

        in = Config.class.getClassLoader().getResourceAsStream("hazelcast-client.xml");
        if (in == null) {
            throw new HazelcastException("Could not load 'hazelcast-client.xml' from classpath");
        }
        return true;
    }

    private boolean loadFromWorkingDirectory() {
        File file = new File("hazelcast-client.xml");
        if (!file.exists()) {
            LOGGER.finest("Could not find 'hazelcast-client.xml' in working directory.");
            return false;
        }

        LOGGER.info("Loading 'hazelcast-client.xml' from working directory.");
        try {
            in = new FileInputStream(file);
        } catch (FileNotFoundException e) {
            throw new HazelcastException("Failed to open file: " + file.getAbsolutePath(), e);
        }
        return true;
    }

    private boolean loadFromSystemProperty() {
        String configSystemProperty = System.getProperty("hazelcast.client.config");

        if (configSystemProperty == null) {
            LOGGER.finest("Could not 'hazelcast.client.config' System property");
            return false;
        }

        LOGGER.info("Loading configuration " + configSystemProperty + " from System property 'hazelcast.client.config'");

        if (configSystemProperty.startsWith("classpath:")) {
            loadSystemPropertyClassPathResource(configSystemProperty);
        } else {
            loadSystemPropertyFileResource(configSystemProperty);
        }
        return true;
    }

    private void loadSystemPropertyFileResource(String configSystemProperty) {
        //it is a file.
        File configurationFile = new File(configSystemProperty);
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
    }

    private void loadSystemPropertyClassPathResource(String configSystemProperty) {
        //it is a explicit configured classpath resource.
        String resource = configSystemProperty.substring("classpath:".length());

        LOGGER.info("Using classpath resource at " + resource);

        if (resource.isEmpty()) {
            throw new HazelcastException("classpath resource can't be empty");
        }

        in = Config.class.getClassLoader().getResourceAsStream(resource);
        if (in == null) {
            throw new HazelcastException("Could not load classpath resource: " + resource);
        }
    }

}
