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
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Config {

	public static final int DEFAULT_PORT = 5701;

	public static final String DEFAULT_GROUP_PASSWORD = "group-pass";

	public static final String DEFAULT_GROUP_NAME = "group-dev";

    private String xmlConfig = null;

    private String groupName = DEFAULT_GROUP_NAME;

    private String groupPassword = DEFAULT_GROUP_PASSWORD;

    private int port = DEFAULT_PORT;

    private boolean reuseAddress = false;

    private boolean portAutoIncrement = true;

    private ExecutorConfig executorConfig = new ExecutorConfig();

    private Map<String, TopicConfig> mapTopicConfigs = new ConcurrentHashMap<String, TopicConfig>();

    private Map<String, QueueConfig> mapQueueConfigs = new ConcurrentHashMap<String, QueueConfig>();

    private Map<String, MapConfig> mapMapConfigs = new ConcurrentHashMap<String, MapConfig>();
    
    private URL configurationUrl;
    
    private File configurationFile;
    
    private NetworkConfig networkConfig = new NetworkConfig();

    private boolean superClient = false;

    private ClassLoader classLoader = null;
    
    public Config() {
        final String superClientProp = System.getProperty("hazelcast.super.client");
        if (superClientProp != null) {
            if ("true".equalsIgnoreCase(superClientProp)) {
                superClient = true;
            }
        }
        String os = System.getProperty("os.name").toLowerCase();
		reuseAddress = (os.indexOf( "win" ) == -1);
    }

    public ClassLoader getClassLoader() {
        if (classLoader == null) {
            return Thread.currentThread().getContextClassLoader();
        }
        return classLoader;
    }

    public void setClassLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
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
            return indextSecondPart != -1;

        }
    }

    public NetworkConfig getNetworkConfig() {
        return networkConfig;
    }

    public void setNetworkConfig(NetworkConfig networkConfig) {
        this.networkConfig = networkConfig;
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


    public boolean isReuseAddress() {
        return reuseAddress;
    }

    public void setReuseAddress(boolean reuseAddress) {
        this.reuseAddress = reuseAddress;
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

    public boolean isSuperClient() {
        return superClient;
    }

    public void setSuperClient(boolean superClient) {
        this.superClient = superClient;
    }
}
