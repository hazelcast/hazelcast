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

package com.hazelcast.datalink.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.DataLinkConfig;
import com.hazelcast.datalink.DataLink;
import com.hazelcast.datalink.JdbcDataLink;
import com.hazelcast.datalink.DataLinkResource;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

public final class DataLinkTestUtil {

    private DataLinkTestUtil() {
    }

    public static void configureJdbcDataLink(String name, String jdbcUrl, Config config) {
        Properties properties = new Properties();
        properties.put("jdbcUrl", jdbcUrl);
        DataLinkConfig dataLinkConfig = new DataLinkConfig()
                .setName(name)
                .setClassName(JdbcDataLink.class.getName())
                .setProperties(properties);
        config.getDataLinkConfigs().put(name, dataLinkConfig);
    }

    public static void configureJdbcDataLink(String name, String jdbcUrl, String username, String password, Config config) {
        Properties properties = new Properties();
        properties.put("jdbcUrl", jdbcUrl);
        properties.put("username", username);
        properties.put("password", password);
        DataLinkConfig dataLinkConfig = new DataLinkConfig()
                .setName(name)
                .setClassName(JdbcDataLink.class.getName())
                .setProperties(properties);
        config.getDataLinkConfigs().put(name, dataLinkConfig);
    }

    public static void configureDummyDataLink(String name, Config config) {
        DataLinkConfig dataLinkConfig = new DataLinkConfig()
                .setName(name)
                .setClassName(DummyDataLink.class.getName());
        config.getDataLinkConfigs().put(name, dataLinkConfig);
    }

    public static class DummyDataLink implements DataLink {

        private final DataLinkConfig config;
        private boolean closed;

        public DummyDataLink(DataLinkConfig config) {
            this.config = config;
        }

        @Override
        public String getName() {
            return config.getName();
        }

        @Override
        public List<DataLinkResource> listResources() {
            return Collections.emptyList();
        }

        @Override
        public DataLinkConfig getConfig() {
            return config;
        }

        @Override
        public void retain() {

        }

        public Object getDataLink() {
            return new Object();
        }

        @Override
        public void close() throws Exception {
            closed = true;
        }

        public boolean isClosed() {
            return closed;
        }
    }
}
