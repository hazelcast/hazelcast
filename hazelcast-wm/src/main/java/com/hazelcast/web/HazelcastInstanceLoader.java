/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.web;

import com.hazelcast.client.ClientConfig;
import com.hazelcast.client.ClientConfigBuilder;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigLoader;
import com.hazelcast.config.UrlXmlConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.DuplicateInstanceNameException;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;
import java.util.logging.Level;

class HazelcastInstanceLoader {

    private final static ILogger logger = Logger.getLogger(HazelcastInstanceLoader.class.getName());

    public static HazelcastInstance createInstance(Properties properties) throws ServletException {
        final String instanceName = properties.getProperty("instance-name");
        final String configLocation = properties.getProperty("config-location");
        final String useClientProp = properties.getProperty("use-client");
        final String clientConfigLocation = properties.getProperty("client-config-location");

        URL configUrl = null;

        if (!isEmpty(clientConfigLocation)) {
            configUrl = getConfigURL(clientConfigLocation);
        } else if(!isEmpty(configLocation)) {
            configUrl = getConfigURL(configLocation);
        }

        return getInstance(configUrl, instanceName, useClientProp);
    }

    public static HazelcastInstance createInstance(FilterConfig filterConfig) throws ServletException {
        final String instanceName = filterConfig.getInitParameter("instance-name");
        final String configLocation = filterConfig.getInitParameter("config-location");
        final String useClientProp = filterConfig.getInitParameter("use-client");
        final String clientConfigLocation = filterConfig.getInitParameter("client-config-location");

        URL configUrl = null;

        if(!isEmpty(clientConfigLocation)) {
            configUrl = getConfigURL(filterConfig, clientConfigLocation);
        } else if(!isEmpty(configLocation)) {
            configUrl = getConfigURL(filterConfig, configLocation);
        }

        return getInstance(configUrl, instanceName, useClientProp);
    }

    private static HazelcastInstance getInstance(URL configUrl, String instanceName, String useClientProp)
            throws ServletException {
        if(!isEmpty(useClientProp) && Boolean.parseBoolean(useClientProp)) {
            logger.log(Level.WARNING,
                    "Creating HazelcastClient, make sure this node has access to an already running cluster...");
            ClientConfig clientConfig ;
            if (configUrl == null) {
                clientConfig = new ClientConfig();
                clientConfig.setUpdateAutomatic(true);
                clientConfig.setInitialConnectionAttemptLimit(3);
                clientConfig.setReconnectionAttemptLimit(5);
            } else {
                try {
                    clientConfig = new ClientConfigBuilder(configUrl).build();
                } catch (IOException e) {
                    throw new ServletException(e);
                }
            }
            return HazelcastClient.newHazelcastClient(clientConfig);
        }

        if (configUrl == null && isEmpty(instanceName)) {
            return Hazelcast.getDefaultInstance();
        }

        Config config;

        if (configUrl == null) {
            config = new XmlConfigBuilder().build();
        } else {
            try {
                config = new UrlXmlConfig(configUrl);
            } catch (IOException e) {
                throw new ServletException(e);
            }
        }

        if (instanceName != null) {
            config.setInstanceName(instanceName);
            HazelcastInstance instance = Hazelcast.getHazelcastInstanceByName(instanceName);
            if (instance == null) {
                try {
                    instance = Hazelcast.newHazelcastInstance(config);
                } catch (DuplicateInstanceNameException ignored) {
                    instance = Hazelcast.getHazelcastInstanceByName(instanceName);
                }
            }
            return instance;
        } else {
            return Hazelcast.newHazelcastInstance(config);
        }
    }

    private static URL getConfigURL(final FilterConfig filterConfig, final String configLocation) {
        URL configUrl = null;
        try {
            configUrl = filterConfig.getServletContext().getResource(configLocation);
        } catch (MalformedURLException e) {
        }
        if (configUrl == null) {
            configUrl = ConfigLoader.locateConfig(configLocation);
        }
        return configUrl;
    }

    private static URL getConfigURL(final String configLocation) {
        URL configUrl;
        configUrl = HazelcastInstanceLoader.class.getResource(configLocation);

        if (configUrl == null) {
            configUrl = ConfigLoader.locateConfig(configLocation);
        }

        return configUrl;
    }

    private static boolean isEmpty(String s) {
        return s == null || s.trim().length() == 0;
    }
}