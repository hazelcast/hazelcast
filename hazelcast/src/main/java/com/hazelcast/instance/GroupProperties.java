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

package com.hazelcast.instance;

import com.hazelcast.config.Config;
import com.hazelcast.util.HealthMonitorLevel;

/**
 * The GroupProperties contain the Hazelcast properties. They can be set as an environmental variable, or
 * directly on the Config using {@link Config#setProperty(String, String)} or from the XML.
 */
public class GroupProperties {

    public static final String PROP_HOSTED_MANAGEMENT_ENABLED = "hazelcast.hosted.management.enabled";
    public static final String PROP_HOSTED_MANAGEMENT_URL = "hazelcast.hosted.management.url";
    public static final String PROP_HEALTH_MONITORING_LEVEL = "hazelcast.health.monitoring.level";
    public static final String PROP_HEALTH_MONITORING_DELAY_SECONDS = "hazelcast.health.monitoring.delay.seconds";
    public static final String PROP_VERSION_CHECK_ENABLED = "hazelcast.version.check.enabled";
    public static final String PROP_PREFER_IPv4_STACK = "hazelcast.prefer.ipv4.stack";
    public static final String PROP_IO_THREAD_COUNT = "hazelcast.io.thread.count";
    /**
     * The number of partition threads per Member. If this is less than the number of partitions on a Member, then
     * partition operations will queue behind other operations of different partitions. The default is 4.
     */
    public static final String PROP_PARTITION_OPERATION_THREAD_COUNT = "hazelcast.operation.thread.count";
    public static final String PROP_GENERIC_OPERATION_THREAD_COUNT = "hazelcast.operation.generic.thread.count";
    public static final String PROP_EVENT_THREAD_COUNT = "hazelcast.event.thread.count";
    public static final String PROP_EVENT_QUEUE_CAPACITY = "hazelcast.event.queue.capacity";
    public static final String PROP_EVENT_QUEUE_TIMEOUT_MILLIS = "hazelcast.event.queue.timeout.millis";
    public static final String PROP_CONNECT_ALL_WAIT_SECONDS = "hazelcast.connect.all.wait.seconds";
    public static final String PROP_MEMCACHE_ENABLED = "hazelcast.memcache.enabled";
    public static final String PROP_REST_ENABLED = "hazelcast.rest.enabled";
    public static final String PROP_MAP_LOAD_CHUNK_SIZE = "hazelcast.map.load.chunk.size";
    public static final String PROP_MERGE_FIRST_RUN_DELAY_SECONDS = "hazelcast.merge.first.run.delay.seconds";
    public static final String PROP_MERGE_NEXT_RUN_DELAY_SECONDS = "hazelcast.merge.next.run.delay.seconds";
    public static final String PROP_OPERATION_CALL_TIMEOUT_MILLIS = "hazelcast.operation.call.timeout.millis";
    public static final String PROP_SOCKET_BIND_ANY = "hazelcast.socket.bind.any";
    public static final String PROP_SOCKET_SERVER_BIND_ANY = "hazelcast.socket.server.bind.any";
    public static final String PROP_SOCKET_CLIENT_BIND_ANY = "hazelcast.socket.client.bind.any";
    public static final String PROP_SOCKET_CLIENT_BIND = "hazelcast.socket.client.bind";
    public static final String PROP_SOCKET_RECEIVE_BUFFER_SIZE = "hazelcast.socket.receive.buffer.size";
    public static final String PROP_SOCKET_SEND_BUFFER_SIZE = "hazelcast.socket.send.buffer.size";
    public static final String PROP_SOCKET_LINGER_SECONDS = "hazelcast.socket.linger.seconds";
    public static final String PROP_SOCKET_KEEP_ALIVE = "hazelcast.socket.keep.alive";
    public static final String PROP_SOCKET_NO_DELAY = "hazelcast.socket.no.delay";
    public static final String PROP_SHUTDOWNHOOK_ENABLED = "hazelcast.shutdownhook.enabled";
    public static final String PROP_WAIT_SECONDS_BEFORE_JOIN = "hazelcast.wait.seconds.before.join";
    public static final String PROP_MAX_WAIT_SECONDS_BEFORE_JOIN = "hazelcast.max.wait.seconds.before.join";
    public static final String PROP_MAX_JOIN_SECONDS = "hazelcast.max.join.seconds";
    public static final String PROP_MAX_JOIN_MERGE_TARGET_SECONDS = "hazelcast.max.join.merge.target.seconds";
    public static final String PROP_HEARTBEAT_INTERVAL_SECONDS = "hazelcast.heartbeat.interval.seconds";
    public static final String PROP_MAX_NO_HEARTBEAT_SECONDS = "hazelcast.max.no.heartbeat.seconds";
    public static final String PROP_MAX_NO_MASTER_CONFIRMATION_SECONDS = "hazelcast.max.no.master.confirmation.seconds";
    public static final String PROP_MASTER_CONFIRMATION_INTERVAL_SECONDS
            = "hazelcast.master.confirmation.interval.seconds";
    public static final String PROP_MEMBER_LIST_PUBLISH_INTERVAL_SECONDS
            = "hazelcast.member.list.publish.interval.seconds";
    public static final String PROP_ICMP_ENABLED = "hazelcast.icmp.enabled";
    public static final String PROP_ICMP_TIMEOUT = "hazelcast.icmp.timeout";
    public static final String PROP_ICMP_TTL = "hazelcast.icmp.ttl";
    public static final String PROP_INITIAL_MIN_CLUSTER_SIZE = "hazelcast.initial.min.cluster.size";
    public static final String PROP_INITIAL_WAIT_SECONDS = "hazelcast.initial.wait.seconds";
    public static final String PROP_MAP_REPLICA_WAIT_SECONDS_FOR_SCHEDULED_OPERATIONS
            = "hazelcast.map.replica.wait.seconds.for.scheduled.tasks";
    public static final String PROP_PARTITION_COUNT = "hazelcast.partition.count";
    public static final String PROP_LOGGING_TYPE = "hazelcast.logging.type";
    public static final String PROP_ENABLE_JMX = "hazelcast.jmx";
    public static final String PROP_ENABLE_JMX_DETAILED = "hazelcast.jmx.detailed";
    public static final String PROP_MC_MAX_VISIBLE_INSTANCE_COUNT = "hazelcast.mc.max.visible.instance.count";
    public static final String PROP_MC_URL_CHANGE_ENABLED = "hazelcast.mc.url.change.enabled";
    public static final String PROP_CONNECTION_MONITOR_INTERVAL = "hazelcast.connection.monitor.interval";
    public static final String PROP_CONNECTION_MONITOR_MAX_FAULTS = "hazelcast.connection.monitor.max.faults";
    public static final String PROP_PARTITION_MIGRATION_INTERVAL = "hazelcast.partition.migration.interval";
    public static final String PROP_PARTITION_MIGRATION_TIMEOUT = "hazelcast.partition.migration.timeout";
    public static final String PROP_PARTITION_MIGRATION_ZIP_ENABLED = "hazelcast.partition.migration.zip.enabled";
    public static final String PROP_PARTITION_TABLE_SEND_INTERVAL = "hazelcast.partition.table.send.interval";
    public static final String PROP_PARTITION_BACKUP_SYNC_INTERVAL = "hazelcast.partition.backup.sync.interval";
    public static final String PROP_PARTITIONING_STRATEGY_CLASS = "hazelcast.partitioning.strategy.class";
    public static final String PROP_GRACEFUL_SHUTDOWN_MAX_WAIT = "hazelcast.graceful.shutdown.max.wait";
    public static final String PROP_SYSTEM_LOG_ENABLED = "hazelcast.system.log.enabled";
    public static final String PROP_ELASTIC_MEMORY_ENABLED = "hazelcast.elastic.memory.enabled";
    public static final String PROP_ELASTIC_MEMORY_TOTAL_SIZE = "hazelcast.elastic.memory.total.size";
    public static final String PROP_ELASTIC_MEMORY_CHUNK_SIZE = "hazelcast.elastic.memory.chunk.size";
    public static final String PROP_ELASTIC_MEMORY_SHARED_STORAGE = "hazelcast.elastic.memory.shared.storage";
    public static final String PROP_ELASTIC_MEMORY_UNSAFE_ENABLED = "hazelcast.elastic.memory.unsafe.enabled";
    public static final String PROP_ENTERPRISE_LICENSE_KEY = "hazelcast.enterprise.license.key";

    /**
     * This property will only be used temporary until we have exposed the hosted management center to the public.
     * So it will be disabled by default.
     */
    public final GroupProperty HOSTED_MANAGEMENT_ENABLED;
    public final GroupProperty HOSTED_MANAGEMENT_URL;

    public final GroupProperty PARTITION_OPERATION_THREAD_COUNT;
    public final GroupProperty GENERIC_OPERATION_THREAD_COUNT;

    public final GroupProperty EVENT_THREAD_COUNT;

    public final GroupProperty HEALTH_MONITORING_LEVEL;

    public final GroupProperty HEALTH_MONITORING_DELAY_SECONDS;

    public final GroupProperty IO_THREAD_COUNT;

    public final GroupProperty EVENT_QUEUE_CAPACITY;

    public final GroupProperty EVENT_QUEUE_TIMEOUT_MILLIS;

    public final GroupProperty PREFER_IPv4_STACK;

    public final GroupProperty CONNECT_ALL_WAIT_SECONDS;

    public final GroupProperty VERSION_CHECK_ENABLED;

    public final GroupProperty MEMCACHE_ENABLED;

    public final GroupProperty REST_ENABLED;

    public final GroupProperty MAP_LOAD_CHUNK_SIZE;

    public final GroupProperty MERGE_FIRST_RUN_DELAY_SECONDS;

    public final GroupProperty MERGE_NEXT_RUN_DELAY_SECONDS;

    public final GroupProperty OPERATION_CALL_TIMEOUT_MILLIS;

    public final GroupProperty SOCKET_SERVER_BIND_ANY;

    public final GroupProperty SOCKET_CLIENT_BIND_ANY;

    public final GroupProperty SOCKET_CLIENT_BIND;

    // number of kilobytes
    public final GroupProperty SOCKET_RECEIVE_BUFFER_SIZE;

    // number of kilobytes
    public final GroupProperty SOCKET_SEND_BUFFER_SIZE;

    public final GroupProperty SOCKET_LINGER_SECONDS;

    public final GroupProperty SOCKET_KEEP_ALIVE;

    public final GroupProperty SOCKET_NO_DELAY;

    public final GroupProperty SHUTDOWNHOOK_ENABLED;

    public final GroupProperty WAIT_SECONDS_BEFORE_JOIN;

    public final GroupProperty MAX_WAIT_SECONDS_BEFORE_JOIN;

    public final GroupProperty MAX_JOIN_SECONDS;

    public final GroupProperty MAX_JOIN_MERGE_TARGET_SECONDS;

    public final GroupProperty MAX_NO_HEARTBEAT_SECONDS;

    public final GroupProperty HEARTBEAT_INTERVAL_SECONDS;

    public final GroupProperty MASTER_CONFIRMATION_INTERVAL_SECONDS;

    public final GroupProperty MAX_NO_MASTER_CONFIRMATION_SECONDS;

    public final GroupProperty MEMBER_LIST_PUBLISH_INTERVAL_SECONDS;

    public final GroupProperty ICMP_ENABLED;

    public final GroupProperty ICMP_TIMEOUT;

    public final GroupProperty ICMP_TTL;

    public final GroupProperty INITIAL_WAIT_SECONDS;

    public final GroupProperty INITIAL_MIN_CLUSTER_SIZE;

    public final GroupProperty MAP_REPLICA_WAIT_SECONDS_FOR_SCHEDULED_TASKS;

    public final GroupProperty PARTITION_COUNT;

    public final GroupProperty LOGGING_TYPE;

    public final GroupProperty ENABLE_JMX;

    public final GroupProperty ENABLE_JMX_DETAILED;

    public final GroupProperty MC_MAX_INSTANCE_COUNT;

    public final GroupProperty MC_URL_CHANGE_ENABLED;

    public final GroupProperty CONNECTION_MONITOR_INTERVAL;

    public final GroupProperty CONNECTION_MONITOR_MAX_FAULTS;

    public final GroupProperty PARTITION_MIGRATION_INTERVAL;

    public final GroupProperty PARTITION_MIGRATION_TIMEOUT;

    public final GroupProperty PARTITION_MIGRATION_ZIP_ENABLED;

    public final GroupProperty PARTITION_TABLE_SEND_INTERVAL;

    public final GroupProperty PARTITION_BACKUP_SYNC_INTERVAL;

    public final GroupProperty PARTITIONING_STRATEGY_CLASS;

    public final GroupProperty GRACEFUL_SHUTDOWN_MAX_WAIT;

    public final GroupProperty SYSTEM_LOG_ENABLED;

    public final GroupProperty ELASTIC_MEMORY_ENABLED;

    public final GroupProperty ELASTIC_MEMORY_TOTAL_SIZE;

    public final GroupProperty ELASTIC_MEMORY_CHUNK_SIZE;

    public final GroupProperty ELASTIC_MEMORY_SHARED_STORAGE;

    public final GroupProperty ELASTIC_MEMORY_UNSAFE_ENABLED;

    public final GroupProperty ENTERPRISE_LICENSE_KEY;

    /**
     *
     * @param config
     */
    public GroupProperties(Config config) {
        HOSTED_MANAGEMENT_ENABLED = new GroupProperty(config, PROP_HOSTED_MANAGEMENT_ENABLED, "false");

        //todo: we need to pull out the version.
        HOSTED_MANAGEMENT_URL
                = new GroupProperty(config, PROP_HOSTED_MANAGEMENT_URL, "http://manage.hazelcast.com/3.2");

        HEALTH_MONITORING_LEVEL
                = new GroupProperty(config, PROP_HEALTH_MONITORING_LEVEL, HealthMonitorLevel.SILENT.toString());
        HEALTH_MONITORING_DELAY_SECONDS = new GroupProperty(config, PROP_HEALTH_MONITORING_DELAY_SECONDS, "30");
        VERSION_CHECK_ENABLED = new GroupProperty(config, PROP_VERSION_CHECK_ENABLED, "true");
        PREFER_IPv4_STACK = new GroupProperty(config, PROP_PREFER_IPv4_STACK, "true");
        IO_THREAD_COUNT = new GroupProperty(config, PROP_IO_THREAD_COUNT, "3");
        //-1 means that the value is worked out dynamically.
        PARTITION_OPERATION_THREAD_COUNT = new GroupProperty(config, PROP_PARTITION_OPERATION_THREAD_COUNT, "-1");
        GENERIC_OPERATION_THREAD_COUNT = new GroupProperty(config, PROP_GENERIC_OPERATION_THREAD_COUNT, "-1");
        EVENT_THREAD_COUNT = new GroupProperty(config, PROP_EVENT_THREAD_COUNT, "5");
        EVENT_QUEUE_CAPACITY = new GroupProperty(config, PROP_EVENT_QUEUE_CAPACITY, "1000000");
        EVENT_QUEUE_TIMEOUT_MILLIS = new GroupProperty(config, PROP_EVENT_QUEUE_TIMEOUT_MILLIS, "250");
        CONNECT_ALL_WAIT_SECONDS = new GroupProperty(config, PROP_CONNECT_ALL_WAIT_SECONDS, "120");
        MEMCACHE_ENABLED = new GroupProperty(config, PROP_MEMCACHE_ENABLED, "true");
        REST_ENABLED = new GroupProperty(config, PROP_REST_ENABLED, "true");
        MAP_LOAD_CHUNK_SIZE = new GroupProperty(config, PROP_MAP_LOAD_CHUNK_SIZE, "1000");
        MERGE_FIRST_RUN_DELAY_SECONDS = new GroupProperty(config, PROP_MERGE_FIRST_RUN_DELAY_SECONDS, "300");
        MERGE_NEXT_RUN_DELAY_SECONDS = new GroupProperty(config, PROP_MERGE_NEXT_RUN_DELAY_SECONDS, "120");
        OPERATION_CALL_TIMEOUT_MILLIS = new GroupProperty(config, PROP_OPERATION_CALL_TIMEOUT_MILLIS, "60000");
        final GroupProperty SOCKET_BIND_ANY = new GroupProperty(config, PROP_SOCKET_BIND_ANY, "true");
        SOCKET_SERVER_BIND_ANY = new GroupProperty(config, PROP_SOCKET_SERVER_BIND_ANY, SOCKET_BIND_ANY);
        SOCKET_CLIENT_BIND_ANY = new GroupProperty(config, PROP_SOCKET_CLIENT_BIND_ANY, SOCKET_BIND_ANY);
        SOCKET_CLIENT_BIND = new GroupProperty(config, PROP_SOCKET_CLIENT_BIND, "true");
        SOCKET_RECEIVE_BUFFER_SIZE = new GroupProperty(config, PROP_SOCKET_RECEIVE_BUFFER_SIZE, "32");
        SOCKET_SEND_BUFFER_SIZE = new GroupProperty(config, PROP_SOCKET_SEND_BUFFER_SIZE, "32");
        SOCKET_LINGER_SECONDS = new GroupProperty(config, PROP_SOCKET_LINGER_SECONDS, "0");
        SOCKET_KEEP_ALIVE = new GroupProperty(config, PROP_SOCKET_KEEP_ALIVE, "true");
        SOCKET_NO_DELAY = new GroupProperty(config, PROP_SOCKET_NO_DELAY, "true");
        SHUTDOWNHOOK_ENABLED = new GroupProperty(config, PROP_SHUTDOWNHOOK_ENABLED, "true");
        WAIT_SECONDS_BEFORE_JOIN = new GroupProperty(config, PROP_WAIT_SECONDS_BEFORE_JOIN, "5");
        MAX_WAIT_SECONDS_BEFORE_JOIN = new GroupProperty(config, PROP_MAX_WAIT_SECONDS_BEFORE_JOIN, "20");
        MAX_JOIN_SECONDS = new GroupProperty(config, PROP_MAX_JOIN_SECONDS, "300");
        MAX_JOIN_MERGE_TARGET_SECONDS = new GroupProperty(config, PROP_MAX_JOIN_MERGE_TARGET_SECONDS, "20");
        HEARTBEAT_INTERVAL_SECONDS = new GroupProperty(config, PROP_HEARTBEAT_INTERVAL_SECONDS, "1");
        MAX_NO_HEARTBEAT_SECONDS = new GroupProperty(config, PROP_MAX_NO_HEARTBEAT_SECONDS, "300");
        MASTER_CONFIRMATION_INTERVAL_SECONDS
                = new GroupProperty(config, PROP_MASTER_CONFIRMATION_INTERVAL_SECONDS, "30");
        MAX_NO_MASTER_CONFIRMATION_SECONDS = new GroupProperty(config, PROP_MAX_NO_MASTER_CONFIRMATION_SECONDS, "300");
        MEMBER_LIST_PUBLISH_INTERVAL_SECONDS
                = new GroupProperty(config, PROP_MEMBER_LIST_PUBLISH_INTERVAL_SECONDS, "300");
        ICMP_ENABLED = new GroupProperty(config, PROP_ICMP_ENABLED, "false");
        ICMP_TIMEOUT = new GroupProperty(config, PROP_ICMP_TIMEOUT, "1000");
        ICMP_TTL = new GroupProperty(config, PROP_ICMP_TTL, "0");
        INITIAL_MIN_CLUSTER_SIZE = new GroupProperty(config, PROP_INITIAL_MIN_CLUSTER_SIZE, "0");
        INITIAL_WAIT_SECONDS = new GroupProperty(config, PROP_INITIAL_WAIT_SECONDS, "0");
        MAP_REPLICA_WAIT_SECONDS_FOR_SCHEDULED_TASKS
                = new GroupProperty(config, PROP_MAP_REPLICA_WAIT_SECONDS_FOR_SCHEDULED_OPERATIONS, "10");
        PARTITION_COUNT = new GroupProperty(config, PROP_PARTITION_COUNT, "271");
        LOGGING_TYPE = new GroupProperty(config, PROP_LOGGING_TYPE, "jdk");
        ENABLE_JMX = new GroupProperty(config, PROP_ENABLE_JMX, "false");
        ENABLE_JMX_DETAILED = new GroupProperty(config, PROP_ENABLE_JMX_DETAILED, "false");
        MC_MAX_INSTANCE_COUNT = new GroupProperty(config, PROP_MC_MAX_VISIBLE_INSTANCE_COUNT, "100");
        MC_URL_CHANGE_ENABLED = new GroupProperty(config, PROP_MC_URL_CHANGE_ENABLED, "true");
        CONNECTION_MONITOR_INTERVAL = new GroupProperty(config, PROP_CONNECTION_MONITOR_INTERVAL, "100");
        CONNECTION_MONITOR_MAX_FAULTS = new GroupProperty(config, PROP_CONNECTION_MONITOR_MAX_FAULTS, "3");
        PARTITION_MIGRATION_INTERVAL = new GroupProperty(config, PROP_PARTITION_MIGRATION_INTERVAL, "0");
        PARTITION_MIGRATION_TIMEOUT = new GroupProperty(config, PROP_PARTITION_MIGRATION_TIMEOUT, "300");
        PARTITION_MIGRATION_ZIP_ENABLED = new GroupProperty(config, PROP_PARTITION_MIGRATION_ZIP_ENABLED, "true");
        PARTITION_TABLE_SEND_INTERVAL = new GroupProperty(config, PROP_PARTITION_TABLE_SEND_INTERVAL, "15");
        PARTITION_BACKUP_SYNC_INTERVAL = new GroupProperty(config, PROP_PARTITION_BACKUP_SYNC_INTERVAL, "30");
        PARTITIONING_STRATEGY_CLASS = new GroupProperty(config, PROP_PARTITIONING_STRATEGY_CLASS, "");
        GRACEFUL_SHUTDOWN_MAX_WAIT = new GroupProperty(config, PROP_GRACEFUL_SHUTDOWN_MAX_WAIT, "600");
        SYSTEM_LOG_ENABLED = new GroupProperty(config, PROP_SYSTEM_LOG_ENABLED, "true");
        ELASTIC_MEMORY_ENABLED = new GroupProperty(config, PROP_ELASTIC_MEMORY_ENABLED, "false");
        ELASTIC_MEMORY_TOTAL_SIZE = new GroupProperty(config, PROP_ELASTIC_MEMORY_TOTAL_SIZE, "128M");
        ELASTIC_MEMORY_CHUNK_SIZE = new GroupProperty(config, PROP_ELASTIC_MEMORY_CHUNK_SIZE, "1K");
        ELASTIC_MEMORY_SHARED_STORAGE = new GroupProperty(config, PROP_ELASTIC_MEMORY_SHARED_STORAGE, "false");
        ELASTIC_MEMORY_UNSAFE_ENABLED = new GroupProperty(config, PROP_ELASTIC_MEMORY_UNSAFE_ENABLED, "false");
        ENTERPRISE_LICENSE_KEY = new GroupProperty(config, PROP_ENTERPRISE_LICENSE_KEY);
    }

    public static class GroupProperty {

        private final String name;
        private final String value;

        GroupProperty(Config config, String name) {
            this(config, name, (String) null);
        }

        GroupProperty(Config config, String name, GroupProperty defaultValue) {
            this(config, name, defaultValue != null ? defaultValue.getString() : null);
        }

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

        @Override
        public String toString() {
            return "GroupProperty [name=" + this.name + ", value=" + this.value + "]";
        }
    }
}
