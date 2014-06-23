package com.hazelcast.config;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URL;

/**
 * Support class for the {@link com.hazelcast.config.XmlConfigBuilder} that locates the XML configuration:
 * <ol>
 *     <li>system property</li>
 *     <li>working directory</li>
 *     <li>classpath</li>
 *     <li>default</li>
 * </ol>
 */
public class XmlConfigLocator {

    private static final ILogger LOGGER = Logger.getLogger(XmlConfigLocator.class);

    private InputStream in;
    private File configurationFile;
    private URL configurationUrl;

    /**
     * Constructs a XmlConfigLocator that tries to find a usable XML configuration file.
     *
     * @throws HazelcastException if there was a problem locating the config-file.
     */
    public XmlConfigLocator() {
        try {
            if (loadFromSystemProperty()) {
                return;
            }

            if (loadFromWorkingDirectory()) {
                return;
            }

            if (loadHazelcastXmlFromClasspath()) {
                return;
            }

            loadDefaultConfigurationFromClasspath();
        } catch (RuntimeException e) {
            throw new HazelcastException(e);
        }
    }

    public InputStream getIn() {
        return in;
    }

    public File getConfigurationFile() {
        return configurationFile;
    }

    public URL getConfigurationUrl() {
        return configurationUrl;
    }

    private void loadDefaultConfigurationFromClasspath() {
        LOGGER.info("Loading 'hazelcast-default.xml' from classpath.");

        configurationUrl = Config.class.getClassLoader().getResource("hazelcast-default.xml");

        if (configurationUrl == null) {
            throw new HazelcastException("Could not find 'hazelcast-default.xml' in the classpath!"
                    + "This may be due to a wrong-packaged or corrupted jar file.");
        }

        in = Config.class.getClassLoader().getResourceAsStream("hazelcast-default.xml");
        if (in == null) {
            throw new HazelcastException("Could not load 'hazelcast-default.xml' from classpath");
        }
    }

    private boolean loadHazelcastXmlFromClasspath() {
        URL url = Config.class.getClassLoader().getResource("hazelcast.xml");
        if (url == null) {
            LOGGER.finest("Could not find 'hazelcast.xml' in classpath.");
            return false;
        }

        LOGGER.info("Loading 'hazelcast.xml' from classpath.");

        configurationUrl = url;
        in = Config.class.getClassLoader().getResourceAsStream("hazelcast.xml");
        if (in == null) {
            throw new HazelcastException("Could not load 'hazelcast.xml' from classpath");
        }
        return true;
    }

    private boolean loadFromWorkingDirectory() {
        File file = new File("hazelcast.xml");
        if (!file.exists()) {
            LOGGER.finest("Could not find 'hazelcast.xml' in working directory.");
            return false;
        }

        LOGGER.info("Loading 'hazelcast.xml' from working directory.");

        configurationFile = file;
        try {
            in = new FileInputStream(file);
        } catch (FileNotFoundException e) {
            throw new HazelcastException("Failed to open file: " + file.getAbsolutePath(), e);
        }
        return true;
    }

    private boolean loadFromSystemProperty() {
        String configSystemProperty = System.getProperty("hazelcast.config");

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
        //it is a file.
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
        configurationUrl = Config.class.getResource(resource);
    }
}
