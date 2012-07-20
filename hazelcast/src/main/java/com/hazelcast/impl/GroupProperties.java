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

package com.hazelcast.impl;

import com.hazelcast.config.Config;

public class GroupProperties {

    public static final String PROP_VERSION_CHECK_ENABLED = "hazelcast.version.check.enabled";
    public static final String PROP_PREFER_IPv4_STACK = "hazelcast.prefer.ipv4.stack";
    public static final String PROP_IO_THREAD_COUNT = "hazelcast.io.thread.count";
    public static final String PROP_CONNECT_ALL_WAIT_SECONDS = "hazelcast.connect.all.wait.seconds";
    public static final String PROP_TOPIC_FLOW_CONTROL_ENABLED = "hazelcast.topic.flow.control.enabled";
    public static final String PROP_MEMCACHE_ENABLED = "hazelcast.memcache.enabled";
    public static final String PROP_REST_ENABLED = "hazelcast.rest.enabled";
    public static final String PROP_MAP_LOAD_CHUNK_SIZE = "hazelcast.map.load.chunk.size";
    public static final String PROP_MAP_LOAD_THREAD_COUNT = "hazelcast.map.load.thread.count";
    public static final String PROP_IN_THREAD_PRIORITY = "hazelcast.in.thread.priority";
    public static final String PROP_OUT_THREAD_PRIORITY = "hazelcast.out.thread.priority";
    public static final String PROP_SERVICE_THREAD_PRIORITY = "hazelcast.service.thread.priority";
    public static final String PROP_MERGE_FIRST_RUN_DELAY_SECONDS = "hazelcast.merge.first.run.delay.seconds";
    public static final String PROP_MERGE_NEXT_RUN_DELAY_SECONDS = "hazelcast.merge.next.run.delay.seconds";
    public static final String PROP_REDO_WAIT_MILLIS = "hazelcast.redo.wait.millis";
    public static final String PROP_SOCKET_BIND_ANY = "hazelcast.socket.bind.any";
    public static final String PROP_SOCKET_RECEIVE_BUFFER_SIZE = "hazelcast.socket.receive.buffer.size";
    public static final String PROP_SOCKET_SEND_BUFFER_SIZE = "hazelcast.socket.send.buffer.size";
    public static final String PROP_SOCKET_LINGER_SECONDS = "hazelcast.socket.linger.seconds";
    public static final String PROP_SOCKET_KEEP_ALIVE = "hazelcast.socket.keep.alive";
    public static final String PROP_SOCKET_NO_DELAY = "hazelcast.socket.no.delay";
    public static final String PROP_SERIALIZER_GZIP_ENABLED = "hazelcast.serializer.gzip.enabled";
    public static final String PROP_SERIALIZER_SHARED = "hazelcast.serializer.shared";
    public static final String PROP_PACKET_VERSION = "hazelcast.packet.version";
    public static final String PROP_SHUTDOWNHOOK_ENABLED = "hazelcast.shutdownhook.enabled";
    public static final String PROP_WAIT_SECONDS_BEFORE_JOIN = "hazelcast.wait.seconds.before.join";
    public static final String PROP_MAX_WAIT_SECONDS_BEFORE_JOIN = "hazelcast.max.wait.seconds.before.join";
    public static final String PROP_MAX_JOIN_SECONDS = "hazelcast.max.join.seconds";
    public static final String PROP_HEARTBEAT_INTERVAL_SECONDS = "hazelcast.heartbeat.interval.seconds";
    public static final String PROP_MAX_NO_HEARTBEAT_SECONDS = "hazelcast.max.no.heartbeat.seconds";
    public static final String PROP_ICMP_ENABLED = "hazelcast.icmp.enabled";
    public static final String PROP_ICMP_TIMEOUT = "hazelcast.icmp.timeout";
    public static final String PROP_ICMP_TTL = "hazelcast.icmp.ttl";
    public static final String PROP_INITIAL_MIN_CLUSTER_SIZE = "hazelcast.initial.min.cluster.size";
    public static final String PROP_INITIAL_WAIT_SECONDS = "hazelcast.initial.wait.seconds";
    public static final String PROP_RESTART_ON_MAX_IDLE = "hazelcast.restart.on.max.idle";
    public static final String PROP_CONCURRENT_MAP_PARTITION_COUNT = "hazelcast.map.partition.count";
    public static final String PROP_REMOVE_DELAY_SECONDS = "hazelcast.map.remove.delay.seconds";
    public static final String PROP_CLEANUP_DELAY_SECONDS = "hazelcast.map.cleanup.delay.seconds";
    public static final String PROP_EXECUTOR_QUERY_THREAD_COUNT = "hazelcast.executor.query.thread.count";
    public static final String PROP_EXECUTOR_EVENT_THREAD_COUNT = "hazelcast.executor.event.thread.count";
    public static final String PROP_EXECUTOR_CLIENT_THREAD_COUNT = "hazelcast.executor.client.thread.count";
    public static final String PROP_EXECUTOR_STORE_THREAD_COUNT = "hazelcast.executor.store.thread.count";
    public static final String PROP_LOGGING_TYPE = "hazelcast.logging.type";
    public static final String PROP_LOG_STATE = "hazelcast.log.state";
    public static final String PROP_ENABLE_JMX = "hazelcast.jmx";
    public static final String PROP_ENABLE_JMX_DETAILED = "hazelcast.jmx.detailed";
    public static final String PROP_MC_ATOMIC_NUMBER_EXCLUDES = "hazelcast.mc.atomicnumber.excludes";
    public static final String PROP_MC_COUNT_DOWN_LATCH_EXCLUDES = "hazelcast.mc.countdownlatch.excludes";
    public static final String PROP_MC_MAP_EXCLUDES = "hazelcast.mc.map.excludes";
    public static final String PROP_MC_QUEUE_EXCLUDES = "hazelcast.mc.queue.excludes";
    public static final String PROP_MC_SEMAPHORE_EXCLUDES = "hazelcast.mc.semaphore.excludes";
    public static final String PROP_MC_TOPIC_EXCLUDES = "hazelcast.mc.topic.excludes";
    public static final String PROP_MC_MAX_VISIBLE_INSTANCE_COUNT = "hazelcast.mc.max.visible.instance.count";
    public static final String PROP_MC_URL_CHANGE_ENABLED = "hazelcast.mc.url.change.enabled";
    public static final String PROP_CONCURRENT_MAP_SIMPLE_RECORD = "hazelcast.map.simple.record";
    public static final String PROP_CONNECTION_MONITOR_INTERVAL = "hazelcast.connection.monitor.interval";
    public static final String PROP_CONNECTION_MONITOR_MAX_FAULTS = "hazelcast.connection.monitor.max.faults";
    public static final String PROP_PARTITION_MIGRATION_INTERVAL = "hazelcast.partition.migration.interval";
    public static final String PROP_IMMEDIATE_BACKUP_INTERVAL = "hazelcast.immediate.backup.interval";
    public static final String PROP_PARTITION_TABLE_SEND_INTERVAL = "hazelcast.partition.table.send.interval";
    public static final String PROP_GRACEFUL_SHUTDOWN_MAX_WAIT = "hazelcast.graceful.shutdown.max.wait";
    public static final String PROP_FORCE_THROW_INTERRUPTED_EXCEPTION = "hazelcast.force.throw.interrupted.exception";
    public static final String PROP_ELASTIC_MEMORY_ENABLED = "hazelcast.elastic.memory.enabled";
    public static final String PROP_ELASTIC_MEMORY_TOTAL_SIZE = "hazelcast.elastic.memory.total.size";
    public static final String PROP_ELASTIC_MEMORY_CHUNK_SIZE = "hazelcast.elastic.memory.chunk.size";
    public static final String PROP_ELASTIC_MEMORY_SHARED_STORAGE = "hazelcast.elastic.memory.shared.storage";
    public static final String PROP_ENTERPRISE_LICENSE_KEY = "hazelcast.enterprise.license.key";

    public static final GroupProperty SERIALIZER_GZIP_ENABLED = new GroupProperty(null, PROP_SERIALIZER_GZIP_ENABLED, "false");

    public static final GroupProperty SERIALIZER_SHARED = new GroupProperty(null, PROP_SERIALIZER_SHARED, "false");

    public static final GroupProperty PACKET_VERSION = new GroupProperty(null, PROP_PACKET_VERSION, "8");

    public final GroupProperty IO_THREAD_COUNT;

    public final GroupProperty PREFER_IPv4_STACK;

    public final GroupProperty TOPIC_FLOW_CONTROL_ENABLED;

    public final GroupProperty CONNECT_ALL_WAIT_SECONDS;

    public final GroupProperty VERSION_CHECK_ENABLED;

    public final GroupProperty MEMCACHE_ENABLED;

    public final GroupProperty REST_ENABLED;

    public final GroupProperty IN_THREAD_PRIORITY;

    public final GroupProperty OUT_THREAD_PRIORITY;

    public final GroupProperty SERVICE_THREAD_PRIORITY;

    public final GroupProperty MAP_LOAD_CHUNK_SIZE;

    public final GroupProperty MAP_LOAD_THREAD_COUNT;

    public final GroupProperty MERGE_FIRST_RUN_DELAY_SECONDS;

    public final GroupProperty MERGE_NEXT_RUN_DELAY_SECONDS;

    public final GroupProperty REDO_WAIT_MILLIS;

    public final GroupProperty SOCKET_BIND_ANY;

    public final GroupProperty SOCKET_RECEIVE_BUFFER_SIZE; // number of kilobytes

    public final GroupProperty SOCKET_SEND_BUFFER_SIZE;    // number of kilobytes

    public final GroupProperty SOCKET_LINGER_SECONDS;

    public final GroupProperty SOCKET_KEEP_ALIVE;

    public final GroupProperty SOCKET_NO_DELAY;

    public final GroupProperty SHUTDOWNHOOK_ENABLED;

    public final GroupProperty WAIT_SECONDS_BEFORE_JOIN;

    public final GroupProperty MAX_WAIT_SECONDS_BEFORE_JOIN;

    public final GroupProperty MAX_JOIN_SECONDS;

    public final GroupProperty MAX_NO_HEARTBEAT_SECONDS;

    public final GroupProperty HEARTBEAT_INTERVAL_SECONDS;

    public final GroupProperty ICMP_ENABLED;

    public final GroupProperty ICMP_TIMEOUT;

    public final GroupProperty ICMP_TTL;

    public final GroupProperty INITIAL_WAIT_SECONDS;

    public final GroupProperty INITIAL_MIN_CLUSTER_SIZE;

    public final GroupProperty RESTART_ON_MAX_IDLE;

    public final GroupProperty CONCURRENT_MAP_PARTITION_COUNT;

    public final GroupProperty REMOVE_DELAY_SECONDS;

    public final GroupProperty CLEANUP_DELAY_SECONDS;

    public final GroupProperty EXECUTOR_QUERY_THREAD_COUNT;

    public final GroupProperty EXECUTOR_EVENT_THREAD_COUNT;

    public final GroupProperty EXECUTOR_CLIENT_THREAD_COUNT;

    public final GroupProperty EXECUTOR_STORE_THREAD_COUNT;

    public final GroupProperty LOG_STATE;

    public final GroupProperty LOGGING_TYPE;

    public final GroupProperty ENABLE_JMX;

    public final GroupProperty ENABLE_JMX_DETAILED;

    public final GroupProperty MC_ATOMIC_NUMBER_EXCLUDES;

    public final GroupProperty MC_COUNT_DOWN_LATCH_EXCLUDES;

    public final GroupProperty MC_MAP_EXCLUDES;

    public final GroupProperty MC_QUEUE_EXCLUDES;

    public final GroupProperty MC_SEMAPHORE_EXCLUDES;

    public final GroupProperty MC_TOPIC_EXCLUDES;

    public final GroupProperty MC_MAX_INSTANCE_COUNT;

    public final GroupProperty MC_URL_CHANGE_ENABLED;

    public final GroupProperty CONCURRENT_MAP_SIMPLE_RECORD;

    public final GroupProperty CONNECTION_MONITOR_INTERVAL;

    public final GroupProperty CONNECTION_MONITOR_MAX_FAULTS;

    public final GroupProperty IMMEDIATE_BACKUP_INTERVAL;

    public final GroupProperty PARTITION_MIGRATION_INTERVAL;

    public final GroupProperty PARTITION_TABLE_SEND_INTERVAL;

    public final GroupProperty GRACEFUL_SHUTDOWN_MAX_WAIT;

    public final GroupProperty FORCE_THROW_INTERRUPTED_EXCEPTION;

    public final GroupProperty ELASTIC_MEMORY_ENABLED;

    public final GroupProperty ELASTIC_MEMORY_TOTAL_SIZE;

    public final GroupProperty ELASTIC_MEMORY_CHUNK_SIZE;

    public final GroupProperty ELASTIC_MEMORY_SHARED_STORAGE;

    public final GroupProperty ENTERPRISE_LICENSE_KEY;

    public GroupProperties(Config config) {
        VERSION_CHECK_ENABLED = new GroupProperty(config, PROP_VERSION_CHECK_ENABLED, "true");
        PREFER_IPv4_STACK = new GroupProperty(config, PROP_PREFER_IPv4_STACK, "true");
        IO_THREAD_COUNT = new GroupProperty(config, PROP_IO_THREAD_COUNT, "3");
        TOPIC_FLOW_CONTROL_ENABLED = new GroupProperty(config, PROP_TOPIC_FLOW_CONTROL_ENABLED, "true");
        CONNECT_ALL_WAIT_SECONDS = new GroupProperty(config, PROP_CONNECT_ALL_WAIT_SECONDS, "120");
        MEMCACHE_ENABLED = new GroupProperty(config, PROP_MEMCACHE_ENABLED, "true");
        REST_ENABLED = new GroupProperty(config, PROP_REST_ENABLED, "true");
        MAP_LOAD_CHUNK_SIZE = new GroupProperty(config, PROP_MAP_LOAD_CHUNK_SIZE, "1000");
        MAP_LOAD_THREAD_COUNT = new GroupProperty(config, PROP_MAP_LOAD_THREAD_COUNT, "40");
        IN_THREAD_PRIORITY = new GroupProperty(config, PROP_IN_THREAD_PRIORITY, "7");
        OUT_THREAD_PRIORITY = new GroupProperty(config, PROP_OUT_THREAD_PRIORITY, "7");
        SERVICE_THREAD_PRIORITY = new GroupProperty(config, PROP_SERVICE_THREAD_PRIORITY, "8");
        MERGE_FIRST_RUN_DELAY_SECONDS = new GroupProperty(config, PROP_MERGE_FIRST_RUN_DELAY_SECONDS, "300");
        MERGE_NEXT_RUN_DELAY_SECONDS = new GroupProperty(config, PROP_MERGE_NEXT_RUN_DELAY_SECONDS, "120");
        REDO_WAIT_MILLIS = new GroupProperty(config, PROP_REDO_WAIT_MILLIS, "500");
        SOCKET_BIND_ANY = new GroupProperty(config, PROP_SOCKET_BIND_ANY, "true");
        SOCKET_RECEIVE_BUFFER_SIZE = new GroupProperty(config, PROP_SOCKET_RECEIVE_BUFFER_SIZE, "32");
        SOCKET_SEND_BUFFER_SIZE = new GroupProperty(config, PROP_SOCKET_SEND_BUFFER_SIZE, "32");
        SOCKET_LINGER_SECONDS = new GroupProperty(config, PROP_SOCKET_LINGER_SECONDS, "0");
        SOCKET_KEEP_ALIVE = new GroupProperty(config, PROP_SOCKET_KEEP_ALIVE, "true");
        SOCKET_NO_DELAY = new GroupProperty(config, PROP_SOCKET_NO_DELAY, "true");
        SHUTDOWNHOOK_ENABLED = new GroupProperty(config, PROP_SHUTDOWNHOOK_ENABLED, "true");
        WAIT_SECONDS_BEFORE_JOIN = new GroupProperty(config, PROP_WAIT_SECONDS_BEFORE_JOIN, "5");
        MAX_WAIT_SECONDS_BEFORE_JOIN = new GroupProperty(config, PROP_MAX_WAIT_SECONDS_BEFORE_JOIN, "20");
        MAX_JOIN_SECONDS = new GroupProperty(config, PROP_MAX_JOIN_SECONDS, "300");
        HEARTBEAT_INTERVAL_SECONDS = new GroupProperty(config, PROP_HEARTBEAT_INTERVAL_SECONDS, "1");
        MAX_NO_HEARTBEAT_SECONDS = new GroupProperty(config, PROP_MAX_NO_HEARTBEAT_SECONDS, "300");
        ICMP_ENABLED = new GroupProperty(config, PROP_ICMP_ENABLED, "false");
        ICMP_TIMEOUT = new GroupProperty(config, PROP_ICMP_TIMEOUT, "1000");
        ICMP_TTL = new GroupProperty(config, PROP_ICMP_TTL, "0");
        INITIAL_MIN_CLUSTER_SIZE = new GroupProperty(config, PROP_INITIAL_MIN_CLUSTER_SIZE, "0");
        INITIAL_WAIT_SECONDS = new GroupProperty(config, PROP_INITIAL_WAIT_SECONDS, "0");
        RESTART_ON_MAX_IDLE = new GroupProperty(config, PROP_RESTART_ON_MAX_IDLE, "false");
        CONCURRENT_MAP_PARTITION_COUNT = new GroupProperty(config, PROP_CONCURRENT_MAP_PARTITION_COUNT, "271");
        REMOVE_DELAY_SECONDS = new GroupProperty(config, PROP_REMOVE_DELAY_SECONDS, "5");
        CLEANUP_DELAY_SECONDS = new GroupProperty(config, PROP_CLEANUP_DELAY_SECONDS, "10");
        EXECUTOR_QUERY_THREAD_COUNT = new GroupProperty(config, PROP_EXECUTOR_QUERY_THREAD_COUNT, "8");
        EXECUTOR_EVENT_THREAD_COUNT = new GroupProperty(config, PROP_EXECUTOR_EVENT_THREAD_COUNT, "16");
        EXECUTOR_CLIENT_THREAD_COUNT = new GroupProperty(config, PROP_EXECUTOR_CLIENT_THREAD_COUNT, "40");
        EXECUTOR_STORE_THREAD_COUNT = new GroupProperty(config, PROP_EXECUTOR_STORE_THREAD_COUNT, "16");
        LOG_STATE = new GroupProperty(config, PROP_LOG_STATE, "false");
        LOGGING_TYPE = new GroupProperty(config, PROP_LOGGING_TYPE, "jdk");
        ENABLE_JMX = new GroupProperty(config, PROP_ENABLE_JMX, "false");
        ENABLE_JMX_DETAILED = new GroupProperty(config, PROP_ENABLE_JMX_DETAILED, "false");
        MC_ATOMIC_NUMBER_EXCLUDES = new GroupProperty(config, PROP_MC_ATOMIC_NUMBER_EXCLUDES, null);
        MC_COUNT_DOWN_LATCH_EXCLUDES = new GroupProperty(config, PROP_MC_COUNT_DOWN_LATCH_EXCLUDES, null);
        MC_MAP_EXCLUDES = new GroupProperty(config, PROP_MC_MAP_EXCLUDES, null);
        MC_QUEUE_EXCLUDES = new GroupProperty(config, PROP_MC_QUEUE_EXCLUDES, null);
        MC_SEMAPHORE_EXCLUDES = new GroupProperty(config, PROP_MC_SEMAPHORE_EXCLUDES, null);
        MC_TOPIC_EXCLUDES = new GroupProperty(config, PROP_MC_TOPIC_EXCLUDES, null);
        MC_MAX_INSTANCE_COUNT = new GroupProperty(config, PROP_MC_MAX_VISIBLE_INSTANCE_COUNT, "100");
        MC_URL_CHANGE_ENABLED = new GroupProperty(config, PROP_MC_URL_CHANGE_ENABLED, "true");
        CONCURRENT_MAP_SIMPLE_RECORD = new GroupProperty(config, PROP_CONCURRENT_MAP_SIMPLE_RECORD, "false");
        CONNECTION_MONITOR_INTERVAL = new GroupProperty(config, PROP_CONNECTION_MONITOR_INTERVAL, "100");
        CONNECTION_MONITOR_MAX_FAULTS = new GroupProperty(config, PROP_CONNECTION_MONITOR_MAX_FAULTS, "3");
        PARTITION_MIGRATION_INTERVAL = new GroupProperty(config, PROP_PARTITION_MIGRATION_INTERVAL, "1");
        IMMEDIATE_BACKUP_INTERVAL = new GroupProperty(config, PROP_IMMEDIATE_BACKUP_INTERVAL, "0");
        PARTITION_TABLE_SEND_INTERVAL = new GroupProperty(config, PROP_PARTITION_TABLE_SEND_INTERVAL, "10");
        GRACEFUL_SHUTDOWN_MAX_WAIT = new GroupProperty(config, PROP_GRACEFUL_SHUTDOWN_MAX_WAIT, "600");
        FORCE_THROW_INTERRUPTED_EXCEPTION = new GroupProperty(config, PROP_FORCE_THROW_INTERRUPTED_EXCEPTION, "false");
        ELASTIC_MEMORY_ENABLED = new GroupProperty(config, PROP_ELASTIC_MEMORY_ENABLED, "false");
        ELASTIC_MEMORY_TOTAL_SIZE = new GroupProperty(config, PROP_ELASTIC_MEMORY_TOTAL_SIZE, "128M");
        ELASTIC_MEMORY_CHUNK_SIZE = new GroupProperty(config, PROP_ELASTIC_MEMORY_CHUNK_SIZE, "1K");
        ELASTIC_MEMORY_SHARED_STORAGE = new GroupProperty(config, PROP_ELASTIC_MEMORY_SHARED_STORAGE, "false");
        ENTERPRISE_LICENSE_KEY = new GroupProperty(config, PROP_ENTERPRISE_LICENSE_KEY, null);
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

        @Override
        public String toString() {
            return "GroupProperty [name=" + this.name + ", value=" + this.value + "]";
        }
    }
}
