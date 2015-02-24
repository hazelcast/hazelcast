/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
     * Client shuffles the given member list to prevent all clients to connect to the same node when
     * this property is set to false. When it is set to true, the client tries to connect to the nodes
     * in the given order.
     */
    public static final String PROP_SHUFFLE_MEMBER_LIST = "hazelcast.client.shuffle.member.list";
    /**
     * Default value of the shuffle member list is true unless the user specifies it explicitly.
     */
    public static final String PROP_SHUFFLE_INITIAL_MEMBER_LIST_DEFAULT = "true";

    /**
     * Client sends heartbeat messages to the members and this is the timeout for this sending operations. If there
     * is not any message passing between the client and member within the given time via this property
     * in milliseconds, the connection will be closed.
     */
    public static final String PROP_HEARTBEAT_TIMEOUT = "hazelcast.client.heartbeat.timeout";
    /**
     * Default value of the heartbeat timeout unless the user specifies it explicitly.
     */
    public static final String PROP_HEARTBEAT_TIMEOUT_DEFAULT = "60000";

    /**
     * Time interval between the heartbeats sent by the client to the nodes.
     */
    public static final String PROP_HEARTBEAT_INTERVAL = "hazelcast.client.heartbeat.interval";
    /**
     * Default value of heartbeat interval unless the user specifies it explicitly.
     */
    public static final String PROP_HEARTBEAT_INTERVAL_DEFAULT = "5000";

    /**
     * Number of the threads to handle the incoming event packets.
     */
    public static final String PROP_EVENT_THREAD_COUNT = "hazelcast.client.event.thread.count";

    /**
     * Default value of the number of threads to handle the incoming event packets.
     */
    public static final String PROP_EVENT_THREAD_COUNT_DEFAULT = "5";

    /**
     * Capacity of the executor that handles the incoming event packets.
     */
    public static final String PROP_EVENT_QUEUE_CAPACITY = "hazelcast.client.event.queue.capacity";

    /**
     * Default value of the capacity of the executor that handles the incoming event packets.
     */
    public static final String PROP_EVENT_QUEUE_CAPACITY_DEFAULT = "1000000";

    /**
     * Time to give up on invocation when a member in the member list is not reachable.
     */
    public static final String PROP_INVOCATION_TIMEOUT_SECONDS = "hazelcast.client.invocation.timeout.seconds";

    /**
     * Default value of invocation timeout seconds.
     */
    public static final String PROP_INVOCATION_TIMEOUT_SECONDS_DEFAULT = "120";


    private final ClientProperty heartbeatTimeout;
    private final ClientProperty heartbeatInterval;
    private final ClientProperty eventThreadCount;
    private final ClientProperty eventQueueCapacity;
    private final ClientProperty invocationTimeout;
    private final ClientProperty shuffleMemberList;


    public ClientProperties(ClientConfig clientConfig) {
        heartbeatTimeout = new ClientProperty(clientConfig, PROP_HEARTBEAT_TIMEOUT, PROP_HEARTBEAT_TIMEOUT_DEFAULT);
        heartbeatInterval = new ClientProperty(clientConfig, PROP_HEARTBEAT_INTERVAL, PROP_HEARTBEAT_INTERVAL_DEFAULT);
        eventThreadCount = new ClientProperty(clientConfig, PROP_EVENT_THREAD_COUNT, PROP_EVENT_THREAD_COUNT_DEFAULT);
        eventQueueCapacity = new ClientProperty(clientConfig, PROP_EVENT_QUEUE_CAPACITY, PROP_EVENT_QUEUE_CAPACITY_DEFAULT);
        invocationTimeout = new ClientProperty(clientConfig, PROP_INVOCATION_TIMEOUT_SECONDS,
                PROP_INVOCATION_TIMEOUT_SECONDS_DEFAULT);
        shuffleMemberList = new ClientProperty(clientConfig, PROP_SHUFFLE_MEMBER_LIST,
                PROP_SHUFFLE_INITIAL_MEMBER_LIST_DEFAULT);
    }

    public ClientProperty getHeartbeatTimeout() {
        return heartbeatTimeout;
    }

    public ClientProperty getHeartbeatInterval() {
        return heartbeatInterval;
    }

    public ClientProperty getEventQueueCapacity() {
        return eventQueueCapacity;
    }

    public ClientProperty getEventThreadCount() {
        return eventThreadCount;
    }

    public ClientProperty getInvocationTimeoutSeconds() {
        return invocationTimeout;
    }

    public ClientProperty getShuffleMemberList() {
        return shuffleMemberList;
    }

    /**
     * A single client property.
     */
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
