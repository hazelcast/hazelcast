/* 
 * Copyright (c) 2007-2009, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Config {

	public static final int DEFAULT_PORT = 5701;

	public static final String DEFAULT_GROUP_PASSWORD = "group-pass";

	public static final String DEFAULT_GROUP_NAME = "group-dev";

	private final static Logger logger = Logger.getLogger(Config.class.getName());

    private static Config instance = new Config();

    private String xmlConfig = null;

    private String groupName = DEFAULT_GROUP_NAME;

    private String groupPassword = DEFAULT_GROUP_PASSWORD;

    private int port = DEFAULT_PORT;

    private boolean portAutoIncrement = true;

    private Interfaces interfaces = new Interfaces();

    private Join join = new Join();

    private ExecutorConfig executorConfig = new ExecutorConfig();

    private Map<String, TopicConfig> mapTopicConfigs = new HashMap<String, TopicConfig>();

    private Map<String, QueueConfig> mapQueueConfigs = new HashMap<String, QueueConfig>();

    private Map<String, MapConfig> mapMapConfigs = new HashMap<String, MapConfig>();

    private boolean usingSystemConfig = false;
    
    private URL configurationUrl;
    
    private File configurationFile;
    
    private ConfigBuilder configBuilder;
    
    private Config() {
        String configFile = System.getProperty("hazelcast.config");
        InputStream in = null;
        try {
            if (configFile != null) {
            	configurationFile = new File(configFile);
                if (!configurationFile.exists()) {
                    String msg = "Config file at '" + configFile + "' doesn't exist.";
                    msg += "\nHazelcast will try to use the hazelcast.xml config file in the classpath.";
                    logger.log(Level.WARNING, msg);
                    configurationFile = null;
                }
            }

            if (configurationFile == null) {
                configFile = "hazelcast.xml";
                configurationFile = new File("hazelcast.xml");
                if (!configurationFile.exists()) {
                	configurationFile = null;
                }
            }

            if (configurationFile != null) {
                logger.log (Level.INFO, "Using configuration file at " + configurationFile.getAbsolutePath());
                try {
                    in = new FileInputStream(configurationFile);
                    configurationUrl = configurationFile.toURI().toURL();
                    usingSystemConfig = true;
                } catch (final Exception e) {
                    String msg = "Having problem reading config file at '" + configFile + "'.";
                    msg += "\nException message: " + e.getMessage();
                    msg += "\nHazelcast will try to use the hazelcast.xml config file in the jar.";
                    logger.log(Level.WARNING, msg);
                    in = null;
                }
            }

            if (in == null) {
            	configurationUrl = Config.class.getClassLoader().getResource("hazelcast.xml");
                if (configurationUrl == null)
                    return;
                in = Config.class.getClassLoader().getResourceAsStream("hazelcast.xml");
                if (in == null) {
                    String msg = "Having problem reading config file hazelcast.xml in the classpath.";
                    msg += "\nHazelcast will start with default configuration.";
                    logger.log(Level.WARNING, msg);
                }
            }
            
            if (in == null) {
                return;
            }

            // TODO: make ConfigBuilder configurable
            configBuilder = new XmlConfigBuilder(in);
            configBuilder.parse(this);
            
        } catch (final Exception e) {
        	logger.log(Level.SEVERE, "Error while creating configuration", e);
        	e.printStackTrace();
        }
    }

    public static Config get() {
        return instance;
    }

    public QueueConfig getQueueConfig(final String name) {
        final Set<String> qNames = mapQueueConfigs.keySet();
        for (final String pattern : qNames) {
            if (nameMatches(name, pattern)) {
                return mapQueueConfigs.get(pattern);
            }
        }
        QueueConfig defaultConfig = mapQueueConfigs.get("default");
        if (defaultConfig == null) {
            defaultConfig = new QueueConfig();
        }
        return defaultConfig;
    }

    public MapConfig getMapConfig(final String name) {
        final Set<String> qNames = mapMapConfigs.keySet();
        for (final String pattern : qNames) {
            if (nameMatches(name, pattern)) {
                return mapMapConfigs.get(pattern);
            }
        }
        MapConfig defaultConfig = mapMapConfigs.get("default");
        if (defaultConfig == null) {
            defaultConfig = new MapConfig();
        }
        return defaultConfig;
    }

    public TopicConfig getTopicConfig(final String name) {
        final Set<String> tNames = mapTopicConfigs.keySet();
        for (final String pattern : tNames) {
            if (nameMatches(name, pattern)) {
                return mapTopicConfigs.get(pattern);
            }
        }
        TopicConfig defaultConfig = mapTopicConfigs.get("default");
        if (defaultConfig == null) {
            defaultConfig = new TopicConfig();
        }
        return defaultConfig;
    }

    private boolean nameMatches(final String name, final String pattern) {
        final int index = pattern.indexOf('*');
        if (index == -1) {
            return name.equals(pattern);
        } else {
            final String firstPart = pattern.substring(0, index);
            final int indexFirstPart = name.indexOf(firstPart, 0);
            if (indexFirstPart == -1) {
                return false;
            }

            final String secondPart = pattern.substring(index + 1);
            final int indextSecondPart = name.indexOf(secondPart, index + 1);
            if (indextSecondPart == -1) {
                return false;
            }
            
            return true;
        }
    }

	/**
	 * @return the xmlConfig
	 */
	public String getXmlConfig() {
		return xmlConfig;
	}

	/**
	 * @param xmlConfig the xmlConfig to set
	 */
	public void setXmlConfig(String xmlConfig) {
		this.xmlConfig = xmlConfig;
	}

	/**
	 * @return the groupName
	 */
	public String getGroupName() {
		return groupName;
	}

	/**
	 * @param groupName the groupName to set
	 */
	public void setGroupName(String groupName) {
		this.groupName = groupName;
	}

	/**
	 * @return the groupPassword
	 */
	public String getGroupPassword() {
		return groupPassword;
	}

	/**
	 * @param groupPassword the groupPassword to set
	 */
	public void setGroupPassword(String groupPassword) {
		this.groupPassword = groupPassword;
	}

	/**
	 * @return the port
	 */
	public int getPort() {
		return port;
	}

	/**
	 * @param port the port to set
	 */
	public void setPort(int port) {
		this.port = port;
	}

	/**
	 * @return the portAutoIncrement
	 */
	public boolean isPortAutoIncrement() {
		return portAutoIncrement;
	}

	/**
	 * @param portAutoIncrement the portAutoIncrement to set
	 */
	public void setPortAutoIncrement(boolean portAutoIncrement) {
		this.portAutoIncrement = portAutoIncrement;
	}

	/**
	 * @return the interfaces
	 */
	public Interfaces getInterfaces() {
		return interfaces;
	}

	/**
	 * @param interfaces the interfaces to set
	 */
	public void setInterfaces(Interfaces interfaces) {
		this.interfaces = interfaces;
	}

	/**
	 * @return the join
	 */
	public Join getJoin() {
		return join;
	}

	/**
	 * @param join the join to set
	 */
	public void setJoin(Join join) {
		this.join = join;
	}

	/**
	 * @return the executorConfig
	 */
	public ExecutorConfig getExecutorConfig() {
		return executorConfig;
	}

	/**
	 * @param executorConfig the executorConfig to set
	 */
	public void setExecutorConfig(ExecutorConfig executorConfig) {
		this.executorConfig = executorConfig;
	}

	/**
	 * @return the mapTopicConfigs
	 */
	public Map<String, TopicConfig> getMapTopicConfigs() {
		return mapTopicConfigs;
	}

	/**
	 * @param mapTopicConfigs the mapTopicConfigs to set
	 */
	public void setMapTopicConfigs(Map<String, TopicConfig> mapTopicConfigs) {
		this.mapTopicConfigs = mapTopicConfigs;
	}

	/**
	 * @return the mapQConfigs
	 */
	public Map<String, QueueConfig> getMapQConfigs() {
		return mapQueueConfigs;
	}

	/**
	 * @param mapQConfigs the mapQConfigs to set
	 */
	public void setMapQConfigs(Map<String, QueueConfig> mapQConfigs) {
		this.mapQueueConfigs = mapQConfigs;
	}

	/**
	 * @return the mapMapConfigs
	 */
	public Map<String, MapConfig> getMapMapConfigs() {
		return mapMapConfigs;
	}

	/**
	 * @param mapMapConfigs the mapMapConfigs to set
	 */
	public void setMapMapConfigs(Map<String, MapConfig> mapMapConfigs) {
		this.mapMapConfigs = mapMapConfigs;
	}

	/**
	 * @return the usingSystemConfig
	 */
	public boolean isUsingSystemConfig() {
		return usingSystemConfig;
	}

	/**
	 * @param usingSystemConfig the usingSystemConfig to set
	 */
	public void setUsingSystemConfig(boolean usingSystemConfig) {
		this.usingSystemConfig = usingSystemConfig;
	}

	/**
	 * @return the configurationUrl
	 */
	public URL getConfigurationUrl() {
		return configurationUrl;
	}

	/**
	 * @param configurationUrl the configurationUrl to set
	 */
	public void setConfigurationUrl(URL configurationUrl) {
		this.configurationUrl = configurationUrl;
	}

	/**
	 * @return the configurationFile
	 */
	public File getConfigurationFile() {
		return configurationFile;
	}

	/**
	 * @param configurationFile the configurationFile to set
	 */
	public void setConfigurationFile(File configurationFile) {
		this.configurationFile = configurationFile;
	}

}
