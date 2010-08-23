/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl;

import com.hazelcast.config.Config;

public class GroupProperties {

    public static final String PROP_MERGE_FIRST_RUN_DELAY_SECONDS = "hazelcast.merge.first.run.delay.seconds";
    public static final String PROP_MERGE_NEXT_RUN_DELAY_SECONDS = "hazelcast.merge.next.run.delay.seconds";
    public static final String PROP_REDO_WAIT_MILLIS = "hazelcast.redo.wait.millis";
    public static final String PROP_SOCKET_BIND_ANY = "hazelcast.socket.bind.any";
    public static final String PROP_SERIALIZER_GZIP_ENABLED = "hazelcast.serializer.gzip.enabled";
    public static final String PROP_SERIALIZER_SHARED = "hazelcast.serializer.shared";
    public static final String PROP_PACKET_VERSION = "hazelcast.packet.version";
    public static final String PROP_SHUTDOWNHOOK_ENABLED = "hazelcast.shutdownhook.enabled";
    public static final String PROP_WAIT_SECONDS_BEFORE_JOIN = "hazelcast.wait.seconds.before.join";
    public static final String PROP_MAX_NO_HEARTBEAT_SECONDS = "hazelcast.max.no.heartbeat.seconds";
    public static final String PROP_INITIAL_WAIT_SECONDS = "hazelcast.initial.wait.seconds";
    public static final String PROP_RESTART_ON_MAX_IDLE = "hazelcast.restart.on.max.idle";
    public static final String PROP_CONCURRENT_MAP_PARTITION_COUNT = "hazelcast.map.partition.count";
    public static final String PROP_BLOCKING_QUEUE_BLOCK_SIZE = "hazelcast.queue.block.size";
    public static final String PROP_REMOVE_DELAY_SECONDS = "hazelcast.map.remove.delay.seconds";
    public static final String PROP_CLEANUP_DELAY_SECONDS = "hazelcast.map.cleanup.delay.seconds";
    public static final String PROP_EXECUTOR_QUERY_THREAD_COUNT = "hazelcast.executor.query.thread.count";
    public static final String PROP_EXECUTOR_EVENT_THREAD_COUNT = "hazelcast.executor.event.thread.count";
    public static final String PROP_EXECUTOR_MIGRATION_THREAD_COUNT = "hazelcast.executor.migration.thread.count";
    public static final String PROP_EXECUTOR_CLIENT_THREAD_COUNT = "hazelcast.executor.client.thread.count";
    public static final String PROP_EXECUTOR_STORE_THREAD_COUNT = "hazelcast.executor.store.thread.count";
    public static final String PROP_LOG_STATE = "hazelcast.log.state";

    public static final GroupProperty SERIALIZER_GZIP_ENABLED = new GroupProperty(null, PROP_SERIALIZER_GZIP_ENABLED, "false");

    public static final GroupProperty SERIALIZER_SHARED = new GroupProperty(null, PROP_SERIALIZER_SHARED, "false");

    public static final GroupProperty PACKET_VERSION = new GroupProperty(null, PROP_PACKET_VERSION, "5");

    public final GroupProperty MERGE_FIRST_RUN_DELAY_SECONDS;

    public final GroupProperty MERGE_NEXT_RUN_DELAY_SECONDS;

    public final GroupProperty REDO_WAIT_MILLIS;

    public final GroupProperty SOCKET_BIND_ANY;

    public final GroupProperty SHUTDOWNHOOK_ENABLED;

    public final GroupProperty WAIT_SECONDS_BEFORE_JOIN;

    public final GroupProperty MAX_NO_HEARTBEAT_SECONDS;

    public final GroupProperty INITIAL_WAIT_SECONDS;

    public final GroupProperty RESTART_ON_MAX_IDLE;

    public final GroupProperty CONCURRENT_MAP_PARTITION_COUNT;

    public final GroupProperty BLOCKING_QUEUE_BLOCK_SIZE;

    public final GroupProperty REMOVE_DELAY_SECONDS;

    public final GroupProperty CLEANUP_DELAY_SECONDS;

    public final GroupProperty EXECUTOR_QUERY_THREAD_COUNT;

    public final GroupProperty EXECUTOR_EVENT_THREAD_COUNT;

    public final GroupProperty EXECUTOR_MIGRATION_THREAD_COUNT;

    public final GroupProperty EXECUTOR_CLIENT_THREAD_COUNT;

    public final GroupProperty EXECUTOR_STORE_THREAD_COUNT;

    public final GroupProperty LOG_STATE;

    public GroupProperties(Config config) {
        MERGE_FIRST_RUN_DELAY_SECONDS = new GroupProperty(config, PROP_MERGE_FIRST_RUN_DELAY_SECONDS, "300");
        MERGE_NEXT_RUN_DELAY_SECONDS = new GroupProperty(config, PROP_MERGE_NEXT_RUN_DELAY_SECONDS, "120");
        REDO_WAIT_MILLIS = new GroupProperty(config, PROP_REDO_WAIT_MILLIS, "500");
        SOCKET_BIND_ANY = new GroupProperty(config, PROP_SOCKET_BIND_ANY, "true");
        SHUTDOWNHOOK_ENABLED = new GroupProperty(config, PROP_SHUTDOWNHOOK_ENABLED, "true");
        WAIT_SECONDS_BEFORE_JOIN = new GroupProperty(config, PROP_WAIT_SECONDS_BEFORE_JOIN, "5");
        MAX_NO_HEARTBEAT_SECONDS = new GroupProperty(config, PROP_MAX_NO_HEARTBEAT_SECONDS, "300");
        INITIAL_WAIT_SECONDS = new GroupProperty(config, PROP_INITIAL_WAIT_SECONDS, "0");
        RESTART_ON_MAX_IDLE = new GroupProperty(config, PROP_RESTART_ON_MAX_IDLE, "false");
        CONCURRENT_MAP_PARTITION_COUNT = new GroupProperty(config, PROP_CONCURRENT_MAP_PARTITION_COUNT, "271");
        BLOCKING_QUEUE_BLOCK_SIZE = new GroupProperty(config, PROP_BLOCKING_QUEUE_BLOCK_SIZE, "1000");
        REMOVE_DELAY_SECONDS = new GroupProperty(config, PROP_REMOVE_DELAY_SECONDS, "5");
        CLEANUP_DELAY_SECONDS = new GroupProperty(config, PROP_CLEANUP_DELAY_SECONDS, "10");
        EXECUTOR_QUERY_THREAD_COUNT = new GroupProperty(config, PROP_EXECUTOR_QUERY_THREAD_COUNT, "8");
        EXECUTOR_EVENT_THREAD_COUNT = new GroupProperty(config, PROP_EXECUTOR_EVENT_THREAD_COUNT, "16");
        EXECUTOR_MIGRATION_THREAD_COUNT = new GroupProperty(config, PROP_EXECUTOR_MIGRATION_THREAD_COUNT, "20");
        EXECUTOR_CLIENT_THREAD_COUNT = new GroupProperty(config, PROP_EXECUTOR_CLIENT_THREAD_COUNT, "40");
        EXECUTOR_STORE_THREAD_COUNT = new GroupProperty(config, PROP_EXECUTOR_STORE_THREAD_COUNT, "16");
        LOG_STATE = new GroupProperty(config, PROP_LOG_STATE, "false");
    }

    public static class GroupProperty {

        private final String name;
        private final String value;

        GroupProperty(Config config, String name, String defaultValue) {
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
    }
}
