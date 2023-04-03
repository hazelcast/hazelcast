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
import com.hazelcast.datalink.DataLinkBase;
import com.hazelcast.datalink.DataLinkRegistration;
import com.hazelcast.datalink.DataLinkResource;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

public final class DataLinkTestUtil {

    private DataLinkTestUtil() {
    }

    public static void configureJdbcDataLink(String name, String jdbcUrl, Config config) {
        Properties properties = new Properties();
        properties.put("jdbcUrl", jdbcUrl);
        DataLinkConfig dataLinkConfig = new DataLinkConfig()
                .setName(name)
                .setType("jdbc")
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
                .setType("jdbc")
                .setProperties(properties);
        config.getDataLinkConfigs().put(name, dataLinkConfig);
    }

    public static void configureDummyDataLink(String name, Config config) {
        DataLinkConfig dataLinkConfig = new DataLinkConfig()
                .setName(name)
                .setType("dummy");
        config.getDataLinkConfigs().put(name, dataLinkConfig);
    }

    public static class DummyDataLink extends DataLinkBase {

        private volatile boolean closed;

        public DummyDataLink(DataLinkConfig config) {
            super(config);
        }

        @Nonnull
        @Override
        public Collection<DataLinkResource> listResources() {
            return Arrays.asList(
                    new DataLinkResource("testType1", "testName1"),
                    new DataLinkResource("testType2", "testName2")
            );
        }

        @Override
        public void destroy() {
            closed = true;
        }

        public boolean isClosed() {
            return closed;
        }
    }

    public static class DummyDataLinkRegistration implements DataLinkRegistration {

        @Override
        public String type() {
            return "DUMMY";
        }

        @Override
        public Class<? extends DataLink> clazz() {
            return DummyDataLink.class;
        }
    }
}
