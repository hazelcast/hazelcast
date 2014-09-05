/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigLoader;
import com.hazelcast.config.UrlXmlConfig;
import com.hazelcast.config.XmlConfigBuilder;
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

import static java.lang.String.format;

final class HazelcastInstanceLoader {

    public static final String INSTANCE_NAME = "instance-name";
    public static final String CONFIG_LOCATION = "config-location";
    public static final String USE_CLIENT = "use-client";
    public static final String CLIENT_CONFIG_LOCATION = "client-config-location";

    private static final ILogger LOGGER = Logger.getLogger(HazelcastInstanceLoader.class);
    private static final int DEFAULT_CONNECTION_ATTEMPT_LIMIT = 3;

    private HazelcastInstanceLoader() {
    }

    public static HazelcastInstance createInstance(final FilterConfig filterConfig, final Properties properties)
            throws ServletException {
        final String instanceName = properties.getProperty(INSTANCE_NAME);
        final String configLocation = properties.getProperty(CONFIG_LOCATION);
        final String useClientProp = properties.getProperty(USE_CLIENT);
        final String clientConfigLocation = properties.getProperty(CLIENT_CONFIG_LOCATION);
        final boolean useClient = !isEmpty(useClientProp) && Boolean.parseBoolean(useClientProp);

        URL configUrl = null;
        if (useClient && !isEmpty(clientConfigLocation)) {
            configUrl = getConfigURL(filterConfig, clientConfigLocation);
        } else if (!isEmpty(configLocation)) {
            configUrl = getConfigURL(filterConfig, configLocation);
        }

        if (useClient) {
            return createClientInstance(configUrl);
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

        return createHazelcastInstance(instanceName, config);
    }

    private static HazelcastInstance createHazelcastInstance(String instanceName, Config config) {
        if (!isEmpty(instanceName)) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info(format("getOrCreateHazelcastInstance for session replication, using name '%s'", instanceName));
            }
            config.setInstanceName(instanceName);
            return Hazelcast.getOrCreateHazelcastInstance(config);
        } else {
            LOGGER.info("Creating a new HazelcastInstance for session replication");
            return Hazelcast.newHazelcastInstance(config);
        }
    }

    private static HazelcastInstance createClientInstance(URL configUrl) throws ServletException {
        LOGGER.warning("Creating HazelcastClient for session replication...");
        LOGGER.warning("make sure this client has access to an already running cluster...");
        ClientConfig clientConfig;
        if (configUrl == null) {
            clientConfig = new ClientConfig();
            clientConfig.getNetworkConfig().setConnectionAttemptLimit(DEFAULT_CONNECTION_ATTEMPT_LIMIT);
        } else {
            try {
                clientConfig = new XmlClientConfigBuilder(configUrl).build();
            } catch (IOException e) {
                throw new ServletException(e);
            }
        }
        return HazelcastClient.newHazelcastClient(clientConfig);
    }

    private static URL getConfigURL(final FilterConfig filterConfig, final String configLocation) throws ServletException {
        URL configUrl = null;
        try {
            configUrl = filterConfig.getServletContext().getResource(configLocation);
        } catch (MalformedURLException ignore) {
            LOGGER.info("ignored MalformedURLException");
        }
        if (configUrl == null) {
            configUrl = ConfigLoader.locateConfig(configLocation);
        }

        if (configUrl == null) {
            throw new ServletException("Could not load configuration '" + configLocation + "'");
        }
        return configUrl;
    }

    private static boolean isEmpty(String s) {
        return s == null || s.trim().length() == 0;
    }

}
