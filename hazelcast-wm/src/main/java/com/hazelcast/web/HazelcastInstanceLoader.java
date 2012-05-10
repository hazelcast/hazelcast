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
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.*;
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
import java.util.Arrays;
import java.util.logging.Level;

class HazelcastInstanceLoader {

    private final static ILogger logger = Logger.getLogger(HazelcastInstanceLoader.class.getName());

    public static HazelcastInstance createInstance(FilterConfig filterConfig) throws ServletException {
        final String instanceName = filterConfig.getInitParameter("instance-name");
        final String configLocation = filterConfig.getInitParameter("config-location");

        final String clientAddresses = filterConfig.getInitParameter("client-addresses");
        final String clientGroup = filterConfig.getInitParameter("client-group");
        final String clientPass = filterConfig.getInitParameter("client-password");

        Config config = null;

                // Join a cluster if we have sufficient configuration info
        if(!isEmpty(clientAddresses) && !isEmpty(clientGroup) && !isEmpty(clientPass)) {
            logger.log(Level.WARNING,
                    "Creating Hazelcast node as Lite-Member. "
                            + "Be sure this node has access to an already running cluster...");
            ClientConfig clientConfig = new ClientConfig();
            clientConfig.setAddresses(Arrays.asList(clientAddresses.split(",")));
            GroupConfig groupConfig = new GroupConfig();
            groupConfig.setName(clientGroup);
            groupConfig.setPassword(clientPass);
            clientConfig.setGroupConfig(groupConfig);
            return HazelcastClient.newHazelcastClient(clientConfig);
        }

        if (isEmpty(configLocation) && isEmpty(instanceName)) {
            return Hazelcast.getDefaultInstance();
        }

        URL configUrl = null;
        if (!isEmpty(configLocation)) {
            try {
                configUrl = filterConfig.getServletContext().getResource(configLocation);
            } catch (MalformedURLException e) {
            }
            if (configUrl == null) {
                configUrl = ConfigLoader.locateConfig(configLocation);
            }
        }
        if (configUrl != null) {
            try {
                config = new UrlXmlConfig(configUrl);
            } catch (IOException e) {
                throw new ServletException(e);
            }
        }
        if (config == null) {
            config = new XmlConfigBuilder().build();
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

    private static boolean isEmpty(String s) {
        return s == null || s.trim().length() == 0;
    }
}