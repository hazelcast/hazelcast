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
 * Client Properties
 */
public class ClientProperties {

    /**
     * Client will be sending heartbeat messages to members and this is the timeout. If there is no any message
     * passing between client and member within the given time via this property in milliseconds the connection
     * will be closed.
     */
    public static final String PROP_HEARTBEAT_TIMEOUT = "hazelcast.client.heartbeat.timeout";
    /**
     * Default value of heartbeat timeout when user not set it explicitly
     */
    public static final String PROP_HEARTBEAT_TIMEOUT_DEFAULT = "60000";

    /**
     * Time interval between heartbeats to nodes from client
     */
    public static final String PROP_HEARTBEAT_INTERVAL = "hazelcast.client.heartbeat.interval";
    /**
     * Default value of PROP_HEARTBEAT_INTERVAL when user not set it explicitly
     */
    public static final String PROP_HEARTBEAT_INTERVAL_DEFAULT = "5000";

    /**
     * Client will retry requests which either inherently retryable(idempotent client)
     * or {@link ClientNetworkConfig#redoOperation} is set to true.
     * <p/>
     * This property is to configure retry count before client give up retrying.
     */
    public static final String PROP_REQUEST_RETRY_COUNT = "hazelcast.client.request.retry.count";
    /**
     * Default value of PROP_REQUEST_RETRY_COUNT when user not set it explicitly
     */
    public static final String PROP_REQUEST_RETRY_COUNT_DEFAULT = "20";

    /**
     * Client will retry requests which either inherently retryable(idempotent client)
     * or {@link ClientNetworkConfig#redoOperation} is set to true.
     * <p/>
     * Time delay in milisecond between retries.
     */
    public static final String PROP_REQUEST_RETRY_WAIT_TIME = "hazelcast.client.request.retry.wait.time";
    /**
     * Default value of PROP_REQUEST_RETRY_WAIT_TIME when user not set it explicitly
     */
    public static final String PROP_REQUEST_RETRY_WAIT_TIME_DEFAULT = "250";

    /**
     * Number of threads to handle incoming event packets
     */
    public static final String PROP_EVENT_THREAD_COUNT = "hazelcast.client.event.thread.count";

    /**
     * Default value of number of threads to handle incoming event packets
     */
    public static final String PROP_EVENT_THREAD_COUNT_DEFAULT = "5";

    /**
     * Capacity of executor that will handle incoming event packets.
     */
    public static final String PROP_EVENT_QUEUE_CAPACITY = "hazelcast.client.event.queue.capacity";

    /**
     * Default value of capacity of executor that will handle incoming event packets.
     */
    public static final String PROP_EVENT_QUEUE_CAPACITY_DEFAULT = "1000000";


    private final ClientProperty heartbeatTimeout;
    private final ClientProperty heartbeatInterval;
    private final ClientProperty retryCount;
    private final ClientProperty retryWaitTime;
    private final ClientProperty eventThreadCount;
    private final ClientProperty eventQueueCapacity;


    public ClientProperties(ClientConfig clientConfig) {
        heartbeatTimeout = new ClientProperty(clientConfig, PROP_HEARTBEAT_TIMEOUT, PROP_HEARTBEAT_TIMEOUT_DEFAULT);
        heartbeatInterval = new ClientProperty(clientConfig, PROP_HEARTBEAT_INTERVAL, PROP_HEARTBEAT_INTERVAL_DEFAULT);
        retryCount = new ClientProperty(clientConfig, PROP_REQUEST_RETRY_COUNT, PROP_REQUEST_RETRY_COUNT_DEFAULT);
        retryWaitTime = new ClientProperty(clientConfig, PROP_REQUEST_RETRY_WAIT_TIME, PROP_REQUEST_RETRY_WAIT_TIME_DEFAULT);
        eventThreadCount = new ClientProperty(clientConfig, PROP_EVENT_THREAD_COUNT, PROP_EVENT_THREAD_COUNT_DEFAULT);
        eventQueueCapacity = new ClientProperty(clientConfig, PROP_EVENT_QUEUE_CAPACITY, PROP_EVENT_QUEUE_CAPACITY_DEFAULT);
    }

    public ClientProperty getHeartbeatTimeout() {
        return heartbeatTimeout;
    }

    public ClientProperty getHeartbeatInterval() {
        return heartbeatInterval;
    }

    public ClientProperty getRetryCount() {
        return retryCount;
    }

    public ClientProperty getRetryWaitTime() {
        return retryWaitTime;
    }

    public ClientProperty getEventQueueCapacity() {
        return eventQueueCapacity;
    }

    public ClientProperty getEventThreadCount() {
        return eventThreadCount;
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
