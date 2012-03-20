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

import com.hazelcast.config.Config;
import com.hazelcast.config.UrlXmlConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.DuplicateInstanceNameException;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import java.io.IOException;
import java.net.URL;

class HazelcastInstanceLoader {

    public static HazelcastInstance createInstance(FilterConfig filterConfig) throws ServletException {
        final String instanceName = filterConfig.getInitParameter("instance-name");
        final String configLocation = filterConfig.getInitParameter("config-location");
        Config config = null;

        if (isEmpty(configLocation) && isEmpty(instanceName)) {
            return Hazelcast.getDefaultInstance();
        }

        URL configUrl = null;
        if (!isEmpty(configLocation)) {
            configUrl = ConfigLoader.locateConfig(filterConfig.getServletContext(), configLocation);
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