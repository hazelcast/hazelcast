/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.config;

/**
 * TODO add javadoc for ClientProperties
 */
public class ClientProperties {


    public static final String PROP_CONNECTION_TIMEOUT = "hazelcast.client.connection.timeout";
    public static final String PROP_CONNECTION_TIMEOUT_DEFAULT = "5000";

    public static final String PROP_HEARTBEAT_INTERVAL = "hazelcast.client.heartbeat.interval";
    public static final String PROP_HEARTBEAT_INTERVAL_DEFAULT = "10000";

    public static final String PROP_MAX_FAILED_HEARTBEAT_COUNT = "hazelcast.client.max.failed.heartbeat.count";
    public static final String PROP_MAX_FAILED_HEARTBEAT_COUNT_DEFAULT = "3";

    public static final String PROP_RETRY_COUNT = "hazelcast.client.retry.count";
    public static final String PROP_RETRY_COUNT_DEFAULT = "20";

    public static final String PROP_RETRY_WAIT_TIME = "hazelcast.client.retry.wait.time";
    public static final String PROP_RETRY_WAIT_TIME_DEFAULT = "250";


    public final ClientProperty CONNECTION_TIMEOUT;
    public final ClientProperty HEARTBEAT_INTERVAL;
    public final ClientProperty MAX_FAILED_HEARTBEAT_COUNT;
    public final ClientProperty RETRY_COUNT;
    public final ClientProperty RETRY_WAIT_TIME;

    public ClientProperties(ClientConfig clientConfig) {
        CONNECTION_TIMEOUT = new ClientProperty(clientConfig, PROP_CONNECTION_TIMEOUT, PROP_CONNECTION_TIMEOUT_DEFAULT);
        HEARTBEAT_INTERVAL = new ClientProperty(clientConfig, PROP_HEARTBEAT_INTERVAL, PROP_HEARTBEAT_INTERVAL_DEFAULT);
        MAX_FAILED_HEARTBEAT_COUNT = new ClientProperty(clientConfig, PROP_MAX_FAILED_HEARTBEAT_COUNT, PROP_MAX_FAILED_HEARTBEAT_COUNT_DEFAULT);
        RETRY_COUNT = new ClientProperty(clientConfig, PROP_RETRY_COUNT, PROP_RETRY_COUNT_DEFAULT);
        RETRY_WAIT_TIME = new ClientProperty(clientConfig, PROP_RETRY_WAIT_TIME, PROP_RETRY_WAIT_TIME_DEFAULT);
    }

    public static class ClientProperty {

        private final String name;
        private final String value;

        ClientProperty(ClientConfig config, String name) {
            this(config, name, (String) null);
        }

        ClientProperty(ClientConfig config, String name, ClientProperty defaultValue) {
            this(config, name, defaultValue != null ? defaultValue.getString() : null);
        }

        ClientProperty(ClientConfig config, String name, String defaultValue) {
            this.name = name;
            String configValue = (config != null) ? config.getProperty(name) : null;
            if (configValue != null) {
                value = configValue;
            } else if (System.getProperty(name) != null) {
                value = System.getProperty(name);
            } else {
                value = defaultValue;
            }
        }

        public String getName() {
            return this.name;
        }

        public String getValue() {
            return value;
        }

        public int getInteger() {
            return Integer.parseInt(this.value);
        }

        public byte getByte() {
            return Byte.parseByte(this.value);
        }

        public boolean getBoolean() {
            return Boolean.valueOf(this.value);
        }

        public String getString() {
            return value;
        }

        public long getLong() {
            return Long.parseLong(this.value);
        }

        @Override
        public String toString() {
            return "ClientProperty [name=" + this.name + ", value=" + this.value + "]";
        }
    }
}
