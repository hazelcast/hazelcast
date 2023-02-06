/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.config.Config;
import com.hazelcast.config.ExternalDataLinkConfig;
import com.hazelcast.datalink.ExternalDataLinkFactory;
import com.hazelcast.datalink.JdbcDataLinkFactory;

import java.util.Properties;

final class ExternalDataLinkTestUtil {
    private ExternalDataLinkTestUtil() {
    }


    static void configureJdbcDataLink(String name, String jdbcUrl, Config config) {
        Properties properties = new Properties();
        properties.put("jdbcUrl", jdbcUrl);
        ExternalDataLinkConfig externalDataLinkConfig = new ExternalDataLinkConfig()
                .setName(name)
                .setClassName(JdbcDataLinkFactory.class.getName())
                .setProperties(properties);
        config.getExternalDataLinkConfigs().put(name, externalDataLinkConfig);
    }


    static void configureJdbcDataLink(String name, String jdbcUrl, String username, String password, Config config) {
        Properties properties = new Properties();
        properties.put("jdbcUrl", jdbcUrl);
        properties.put("username", username);
        properties.put("password", password);
        ExternalDataLinkConfig externalDataLinkConfig = new ExternalDataLinkConfig()
                .setName(name)
                .setClassName(JdbcDataLinkFactory.class.getName())
                .setProperties(properties);
        config.getExternalDataLinkConfigs().put(name, externalDataLinkConfig);
    }

    static void configureDummyDataLink(String name, Config config) {
        ExternalDataLinkConfig externalDataLinkConfig = new ExternalDataLinkConfig()
                .setName(name)
                .setClassName(DummyDataLinkFactory.class.getName());
        config.getExternalDataLinkConfigs().put(name, externalDataLinkConfig);
    }

    private static class DummyDataLinkFactory implements ExternalDataLinkFactory<Object> {

        @Override
        public boolean testConnection() {
            return true;
        }

        @Override
        public Object getDataLink() {
            return new Object();
        }

        @Override
        public void init(ExternalDataLinkConfig config) {

        }
    }
}
