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
import com.hazelcast.config.ExternalDataStoreConfig;
import com.hazelcast.datastore.ExternalDataStoreFactory;
import com.hazelcast.datastore.JdbcDataStoreFactory;

import java.util.Properties;

final class ExternalDataStoreTestUtil {
    private ExternalDataStoreTestUtil() {
    }


    static void configureJdbcDataStore(String name, String jdbcUrl, Config config) {
        Properties properties = new Properties();
        properties.put("jdbcUrl", jdbcUrl);
        ExternalDataStoreConfig externalDataStoreConfig = new ExternalDataStoreConfig()
                .setName(name)
                .setClassName(JdbcDataStoreFactory.class.getName())
                .setProperties(properties);
        config.getExternalDataStoreConfigs().put(name, externalDataStoreConfig);
    }


    static void configureJdbcDataStore(String name, String jdbcUrl, String username, String password, Config config) {
        Properties properties = new Properties();
        properties.put("jdbcUrl", jdbcUrl);
        properties.put("username", username);
        properties.put("password", password);
        ExternalDataStoreConfig externalDataStoreConfig = new ExternalDataStoreConfig()
                .setName(name)
                .setClassName(JdbcDataStoreFactory.class.getName())
                .setProperties(properties);
        config.getExternalDataStoreConfigs().put(name, externalDataStoreConfig);
    }

    static void configureDummyDataStore(String name, Config config) {
        ExternalDataStoreConfig externalDataStoreConfig = new ExternalDataStoreConfig()
                .setName(name)
                .setClassName(DummyDataStoreFactory.class.getName());
        config.getExternalDataStoreConfigs().put(name, externalDataStoreConfig);
    }

    private static class DummyDataStoreFactory implements ExternalDataStoreFactory<Object> {

        @Override
        public boolean testConnection() {
            return true;
        }

        @Override
        public Object getDataStore() {
            return new Object();
        }

        @Override
        public void init(ExternalDataStoreConfig config) {

        }
    }
}
