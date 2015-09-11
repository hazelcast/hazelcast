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

package com.hazelcast.instance;

import com.hazelcast.config.Config;

import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

/**
 * Container for configured Hazelcast properties ({@see GroupProperty}).
 * <p/>
 * A {@link GroupProperty} can be set as:
 * <p><ul>
 * <li>an environmental variable using {@link System#setProperty(String, String)}</li>
 * <li>the programmatic configuration using {@link Config#setProperty(String, String)}</li>
 * <li>the XML configuration
 * {@see http://docs.hazelcast.org/docs/latest-dev/manual/html-single/hazelcast-documentation.html#system-properties}</li>
 * </ul></p>
 * <p/>
 * The old property definitions are deprecated since Hazelcast 3.6. Please use the new {@link GroupProperty} definitions instead.
 */
@SuppressWarnings("unused")
public class GroupProperties {

    @Deprecated
    public static final String PROP_APPLICATION_VALIDATION_TOKEN = GroupProperty.APPLICATION_VALIDATION_TOKEN.getName();
    @Deprecated
    public static final String PROP_HEALTH_MONITORING_LEVEL = GroupProperty.HEALTH_MONITORING_LEVEL.getName();
    @Deprecated
    public static final String PROP_HEALTH_MONITORING_DELAY_SECONDS = GroupProperty.HEALTH_MONITORING_DELAY_SECONDS.getName();
    @Deprecated
    public static final String PROP_PERFORMANCE_MONITOR_ENABLED = GroupProperty.PERFORMANCE_MONITOR_ENABLED.getName();
    @Deprecated
    public static final String PROP_PERFORMANCE_MONITOR_DELAY_SECONDS = GroupProperty.PERFORMANCE_MONITOR_DELAY_SECONDS.getName();
    @Deprecated
    public static final String PROP_PERFORMANCE_MONITOR_MAX_ROLLED_FILE_SIZE_MB
            = GroupProperty.PERFORMANCE_MONITOR_MAX_ROLLED_FILE_SIZE_MB.getName();
    @Deprecated
    public static final String PROP_PERFORMANCE_MONITOR_MAX_ROLLED_FILE_COUNT
            = GroupProperty.PERFORMANCE_MONITOR_MAX_ROLLED_FILE_COUNT.getName();
    @Deprecated
    public static final String PROP_PERFORMANCE_MONITOR_HUMAN_FRIENDLY_FORMAT
            = GroupProperty.PERFORMANCE_MONITOR_HUMAN_FRIENDLY_FORMAT.getName();
    @Deprecated
    public static final String PROP_VERSION_CHECK_ENABLED = GroupProperty.VERSION_CHECK_ENABLED.getName();
    @Deprecated
    public static final String PROP_PREFER_IPv4_STACK = GroupProperty.PREFER_IPv4_STACK.getName();
    @Deprecated
    public static final String PROP_IO_THREAD_COUNT = GroupProperty.IO_THREAD_COUNT.getName();
    @Deprecated
    public static final String PROP_IO_INPUT_THREAD_COUNT = GroupProperty.IO_INPUT_THREAD_COUNT.getName();
    @Deprecated
    public static final String PROP_IO_OUTPUT_THREAD_COUNT = GroupProperty.IO_OUTPUT_THREAD_COUNT.getName();
    @Deprecated
    public static final String PROP_IO_BALANCER_INTERVAL_SECONDS = GroupProperty.IO_BALANCER_INTERVAL_SECONDS.getName();
    @Deprecated
    public static final String PROP_PARTITION_OPERATION_THREAD_COUNT = GroupProperty.PARTITION_OPERATION_THREAD_COUNT.getName();
    @Deprecated
    public static final String PROP_GENERIC_OPERATION_THREAD_COUNT = GroupProperty.GENERIC_OPERATION_THREAD_COUNT.getName();
    @Deprecated
    public static final String PROP_EVENT_THREAD_COUNT = GroupProperty.EVENT_THREAD_COUNT.getName();
    @Deprecated
    public static final String PROP_EVENT_QUEUE_CAPACITY = GroupProperty.EVENT_QUEUE_CAPACITY.getName();
    @Deprecated
    public static final String PROP_EVENT_QUEUE_TIMEOUT_MILLIS = GroupProperty.EVENT_QUEUE_TIMEOUT_MILLIS.getName();
    @Deprecated
    public static final String PROP_CONNECT_ALL_WAIT_SECONDS = GroupProperty.CONNECT_ALL_WAIT_SECONDS.getName();
    @Deprecated
    public static final String PROP_MEMCACHE_ENABLED = GroupProperty.MEMCACHE_ENABLED.getName();
    @Deprecated
    public static final String PROP_REST_ENABLED = GroupProperty.REST_ENABLED.getName();
    @Deprecated
    public static final String PROP_MAP_LOAD_CHUNK_SIZE = GroupProperty.MAP_LOAD_CHUNK_SIZE.getName();
    @Deprecated
    public static final String PROP_MERGE_FIRST_RUN_DELAY_SECONDS = GroupProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName();
    @Deprecated
    public static final String PROP_MERGE_NEXT_RUN_DELAY_SECONDS = GroupProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName();
    @Deprecated
    public static final String PROP_OPERATION_CALL_TIMEOUT_MILLIS = GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName();
    @Deprecated
    public static final String PROP_OPERATION_BACKUP_TIMEOUT_MILLIS = GroupProperty.OPERATION_BACKUP_TIMEOUT_MILLIS.getName();
    @Deprecated
    public static final String PROP_SOCKET_BIND_ANY = GroupProperty.SOCKET_BIND_ANY.getName();
    @Deprecated
    public static final String PROP_SOCKET_SERVER_BIND_ANY = GroupProperty.SOCKET_SERVER_BIND_ANY.getName();
    @Deprecated
    public static final String PROP_SOCKET_CLIENT_BIND_ANY = GroupProperty.SOCKET_CLIENT_BIND_ANY.getName();
    @Deprecated
    public static final String PROP_SOCKET_CLIENT_BIND = GroupProperty.SOCKET_CLIENT_BIND.getName();
    @Deprecated
    public static final String PROP_CLIENT_ENGINE_THREAD_COUNT = GroupProperty.CLIENT_ENGINE_THREAD_COUNT.getName();
    @Deprecated
    public static final String PROP_SOCKET_RECEIVE_BUFFER_SIZE = GroupProperty.SOCKET_RECEIVE_BUFFER_SIZE.getName();
    @Deprecated
    public static final String PROP_SOCKET_SEND_BUFFER_SIZE = GroupProperty.SOCKET_SEND_BUFFER_SIZE.getName();
    @Deprecated
    public static final String PROP_SOCKET_CLIENT_RECEIVE_BUFFER_SIZE = GroupProperty.SOCKET_CLIENT_RECEIVE_BUFFER_SIZE.getName();
    @Deprecated
    public static final String PROP_SOCKET_CLIENT_SEND_BUFFER_SIZE = GroupProperty.SOCKET_CLIENT_SEND_BUFFER_SIZE.getName();
    @Deprecated
    public static final String PROP_SOCKET_LINGER_SECONDS = GroupProperty.SOCKET_LINGER_SECONDS.getName();
    @Deprecated
    public static final String PROP_SOCKET_CONNECT_TIMEOUT_SECONDS = GroupProperty.SOCKET_CONNECT_TIMEOUT_SECONDS.getName();
    @Deprecated
    public static final String PROP_SOCKET_KEEP_ALIVE = GroupProperty.SOCKET_KEEP_ALIVE.getName();
    @Deprecated
    public static final String PROP_SOCKET_NO_DELAY = GroupProperty.SOCKET_NO_DELAY.getName();
    @Deprecated
    public static final String PROP_SHUTDOWNHOOK_ENABLED = GroupProperty.SHUTDOWNHOOK_ENABLED.getName();
    @Deprecated
    public static final String PROP_WAIT_SECONDS_BEFORE_JOIN = GroupProperty.WAIT_SECONDS_BEFORE_JOIN.getName();
    @Deprecated
    public static final String PROP_MAX_WAIT_SECONDS_BEFORE_JOIN = GroupProperty.MAX_WAIT_SECONDS_BEFORE_JOIN.getName();
    @Deprecated
    public static final String PROP_MAX_JOIN_SECONDS = GroupProperty.MAX_JOIN_SECONDS.getName();
    @Deprecated
    public static final String PROP_MAX_JOIN_MERGE_TARGET_SECONDS = GroupProperty.MAX_JOIN_MERGE_TARGET_SECONDS.getName();
    @Deprecated
    public static final String PROP_HEARTBEAT_INTERVAL_SECONDS = GroupProperty.HEARTBEAT_INTERVAL_SECONDS.getName();
    @Deprecated
    public static final String PROP_MAX_NO_HEARTBEAT_SECONDS = GroupProperty.MAX_NO_HEARTBEAT_SECONDS.getName();
    @Deprecated
    public static final String PROP_MAX_NO_MASTER_CONFIRMATION_SECONDS
            = GroupProperty.MAX_NO_MASTER_CONFIRMATION_SECONDS.getName();
    @Deprecated
    public static final String PROP_MASTER_CONFIRMATION_INTERVAL_SECONDS
            = GroupProperty.MASTER_CONFIRMATION_INTERVAL_SECONDS.getName();
    @Deprecated
    public static final String PROP_MEMBER_LIST_PUBLISH_INTERVAL_SECONDS
            = GroupProperty.MEMBER_LIST_PUBLISH_INTERVAL_SECONDS.getName();
    @Deprecated
    public static final String PROP_ICMP_ENABLED = GroupProperty.ICMP_ENABLED.getName();
    @Deprecated
    public static final String PROP_ICMP_TIMEOUT = GroupProperty.ICMP_TIMEOUT.getName();
    @Deprecated
    public static final String PROP_ICMP_TTL = GroupProperty.ICMP_TTL.getName();
    @Deprecated
    public static final String PROP_INITIAL_MIN_CLUSTER_SIZE = GroupProperty.INITIAL_MIN_CLUSTER_SIZE.getName();
    @Deprecated
    public static final String PROP_INITIAL_WAIT_SECONDS = GroupProperty.INITIAL_WAIT_SECONDS.getName();
    @Deprecated
    public static final String PROP_TCP_JOIN_PORT_TRY_COUNT = GroupProperty.TCP_JOIN_PORT_TRY_COUNT.getName();
    @Deprecated
    public static final String PROP_MAP_REPLICA_SCHEDULED_TASK_DELAY_SECONDS
            = GroupProperty.MAP_REPLICA_SCHEDULED_TASK_DELAY_SECONDS.getName();
    @Deprecated
    public static final String PROP_MAP_EXPIRY_DELAY_SECONDS = GroupProperty.MAP_EXPIRY_DELAY_SECONDS.getName();
    @Deprecated
    public static final String PROP_PARTITION_COUNT = GroupProperty.PARTITION_COUNT.getName();
    @Deprecated
    public static final String PROP_LOGGING_TYPE = GroupProperty.LOGGING_TYPE.getName();
    @Deprecated
    public static final String PROP_ENABLE_JMX = GroupProperty.ENABLE_JMX.getName();
    @Deprecated
    public static final String PROP_ENABLE_JMX_DETAILED = GroupProperty.ENABLE_JMX_DETAILED.getName();
    @Deprecated
    public static final String PROP_MC_MAX_VISIBLE_INSTANCE_COUNT = GroupProperty.MC_MAX_VISIBLE_INSTANCE_COUNT.getName();
    @Deprecated
    public static final String PROP_MC_MAX_VISIBLE_SLOW_OPERATION_COUNT
            = GroupProperty.MC_MAX_VISIBLE_SLOW_OPERATION_COUNT.getName();
    @Deprecated
    public static final String PROP_MC_URL_CHANGE_ENABLED = GroupProperty.MC_URL_CHANGE_ENABLED.getName();
    @Deprecated
    public static final String PROP_CONNECTION_MONITOR_INTERVAL = GroupProperty.CONNECTION_MONITOR_INTERVAL.getName();
    @Deprecated
    public static final String PROP_CONNECTION_MONITOR_MAX_FAULTS = GroupProperty.CONNECTION_MONITOR_MAX_FAULTS.getName();
    @Deprecated
    public static final String PROP_PARTITION_MIGRATION_INTERVAL = GroupProperty.PARTITION_MIGRATION_INTERVAL.getName();
    @Deprecated
    public static final String PROP_PARTITION_MIGRATION_TIMEOUT = GroupProperty.PARTITION_MIGRATION_TIMEOUT.getName();
    @Deprecated
    public static final String PROP_PARTITION_MIGRATION_ZIP_ENABLED = GroupProperty.PARTITION_MIGRATION_ZIP_ENABLED.getName();
    @Deprecated
    public static final String PROP_PARTITION_TABLE_SEND_INTERVAL = GroupProperty.PARTITION_TABLE_SEND_INTERVAL.getName();
    @Deprecated
    public static final String PROP_PARTITION_BACKUP_SYNC_INTERVAL = GroupProperty.PARTITION_BACKUP_SYNC_INTERVAL.getName();
    @Deprecated
    public static final String PROP_PARTITION_MAX_PARALLEL_REPLICATIONS
            = GroupProperty.PARTITION_MAX_PARALLEL_REPLICATIONS.getName();
    @Deprecated
    public static final String PROP_PARTITIONING_STRATEGY_CLASS = GroupProperty.PARTITIONING_STRATEGY_CLASS.getName();
    @Deprecated
    public static final String PROP_GRACEFUL_SHUTDOWN_MAX_WAIT = GroupProperty.GRACEFUL_SHUTDOWN_MAX_WAIT.getName();
    @Deprecated
    public static final String PROP_SYSTEM_LOG_ENABLED = GroupProperty.SYSTEM_LOG_ENABLED.getName();
    @Deprecated
    public static final String PROP_LOCK_MAX_LEASE_TIME_SECONDS = GroupProperty.LOCK_MAX_LEASE_TIME_SECONDS.getName();
    @Deprecated
    public static final String PROP_SLOW_OPERATION_DETECTOR_ENABLED = GroupProperty.SLOW_OPERATION_DETECTOR_ENABLED.getName();
    @Deprecated
    public static final String PROP_SLOW_OPERATION_DETECTOR_THRESHOLD_MILLIS
            = GroupProperty.SLOW_OPERATION_DETECTOR_THRESHOLD_MILLIS.getName();
    @Deprecated
    public static final String PROP_SLOW_INVOCATION_DETECTOR_THRESHOLD_MILLIS
            = GroupProperty.SLOW_INVOCATION_DETECTOR_THRESHOLD_MILLIS.getName();
    @Deprecated
    public static final String PROP_SLOW_OPERATION_DETECTOR_LOG_RETENTION_SECONDS
            = GroupProperty.SLOW_OPERATION_DETECTOR_LOG_RETENTION_SECONDS.getName();
    @Deprecated
    public static final String PROP_SLOW_OPERATION_DETECTOR_LOG_PURGE_INTERVAL_SECONDS
            = GroupProperty.SLOW_OPERATION_DETECTOR_LOG_PURGE_INTERVAL_SECONDS
            .getName();
    @Deprecated
    public static final String PROP_SLOW_OPERATION_DETECTOR_STACK_TRACE_LOGGING_ENABLED
            = GroupProperty.SLOW_OPERATION_DETECTOR_STACK_TRACE_LOGGING_ENABLED.getName();
    @Deprecated
    public static final String PROP_ELASTIC_MEMORY_ENABLED = GroupProperty.ELASTIC_MEMORY_ENABLED.getName();
    @Deprecated
    public static final String PROP_ELASTIC_MEMORY_TOTAL_SIZE = GroupProperty.ELASTIC_MEMORY_TOTAL_SIZE.getName();
    @Deprecated
    public static final String PROP_ELASTIC_MEMORY_CHUNK_SIZE = GroupProperty.ELASTIC_MEMORY_CHUNK_SIZE.getName();
    @Deprecated
    public static final String PROP_ELASTIC_MEMORY_SHARED_STORAGE = GroupProperty.ELASTIC_MEMORY_SHARED_STORAGE.getName();
    @Deprecated
    public static final String PROP_ELASTIC_MEMORY_UNSAFE_ENABLED = GroupProperty.ELASTIC_MEMORY_UNSAFE_ENABLED.getName();
    @Deprecated
    public static final String PROP_ENTERPRISE_LICENSE_KEY = GroupProperty.ENTERPRISE_LICENSE_KEY.getName();
    @Deprecated
    public static final String PROP_MAP_WRITE_BEHIND_QUEUE_CAPACITY = GroupProperty.MAP_WRITE_BEHIND_QUEUE_CAPACITY.getName();
    @Deprecated
    public static final String PROP_ENTERPRISE_WAN_REP_QUEUE_CAPACITY = GroupProperty.ENTERPRISE_WAN_REP_QUEUE_CAPACITY.getName();
    @Deprecated
    public static final String PROP_ENTERPRISE_WAN_REP_BATCH_SIZE = GroupProperty.ENTERPRISE_WAN_REP_BATCH_SIZE.getName();
    @Deprecated
    public static final String PROP_ENTERPRISE_WAN_REP_BATCH_FREQUENCY_SECONDS
            = GroupProperty.ENTERPRISE_WAN_REP_BATCH_FREQUENCY_SECONDS.getName();
    @Deprecated
    public static final String PROP_CACHE_INVALIDATION_MESSAGE_BATCH_ENABLED
            = GroupProperty.CACHE_INVALIDATION_MESSAGE_BATCH_ENABLED.getName();
    @Deprecated
    public static final String PROP_CACHE_INVALIDATION_MESSAGE_BATCH_SIZE
            = GroupProperty.CACHE_INVALIDATION_MESSAGE_BATCH_SIZE.getName();
    @Deprecated
    public static final String PROP_CACHE_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS
            = GroupProperty.CACHE_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS.getName();
    @Deprecated
    public static final String PROP_ENTERPRISE_WAN_REP_OP_TIMEOUT_MILLIS
            = GroupProperty.ENTERPRISE_WAN_REP_OP_TIMEOUT_MILLIS.getName();
    @Deprecated
    public static final String PROP_CLIENT_MAX_NO_HEARTBEAT_SECONDS = GroupProperty.CLIENT_HEARTBEAT_TIMEOUT_SECONDS.getName();
    @Deprecated
    public static final String PROP_MIGRATION_MIN_DELAY_ON_MEMBER_REMOVED_SECONDS
            = GroupProperty.MIGRATION_MIN_DELAY_ON_MEMBER_REMOVED_SECONDS.getName();
    @Deprecated
    public static final String PROP_BACKPRESSURE_ENABLED = GroupProperty.BACKPRESSURE_ENABLED.getName();
    @Deprecated
    public static final String PROP_BACKPRESSURE_SYNCWINDOW = GroupProperty.BACKPRESSURE_SYNCWINDOW.getName();
    @Deprecated
    public static final String PROP_BACKPRESSURE_BACKOFF_TIMEOUT_MILLIS
            = GroupProperty.BACKPRESSURE_BACKOFF_TIMEOUT_MILLIS.getName();
    @Deprecated
    public static final String PROP_BACKPRESSURE_MAX_CONCURRENT_INVOCATIONS_PER_PARTITION
            = GroupProperty.BACKPRESSURE_MAX_CONCURRENT_INVOCATIONS_PER_PARTITION.getName();
    @Deprecated
    public static final String PROP_QUERY_PREDICATE_PARALLEL_EVALUATION
            = GroupProperty.QUERY_PREDICATE_PARALLEL_EVALUATION.getName();
    @Deprecated
    public static final String PROP_JCACHE_PROVIDER_TYPE = GroupProperty.JCACHE_PROVIDER_TYPE.getName();
    @Deprecated
    public static final String PROP_QUERY_RESULT_SIZE_LIMIT = GroupProperty.QUERY_RESULT_SIZE_LIMIT.getName();
    @Deprecated
    public static final String PROP_QUERY_MAX_LOCAL_PARTITION_LIMIT_FOR_PRE_CHECK
            = GroupProperty.QUERY_MAX_LOCAL_PARTITION_LIMIT_FOR_PRE_CHECK.getName();

    private final String[] properties = new String[GroupProperty.values().length];

    /**
     * Creates a container with configured Hazelcast properties.
     * <p/>
     * Uses the environmental value if no value is defined in the configuration.
     * Uses the default value if no environmental value is defined.
     *
     * @param config {@link Config} used to configure the {@link GroupProperty} values.
     */
    public GroupProperties(Config config) {
        for (GroupProperty groupProperty : GroupProperty.values()) {
            String configValue = (config != null) ? config.getProperty(groupProperty) : null;
            if (configValue != null) {
                properties[groupProperty.ordinal()] = configValue;
                continue;
            }
            String propertyValue = groupProperty.getSystemProperty();
            if (propertyValue != null) {
                properties[groupProperty.ordinal()] = propertyValue;
                continue;
            }
            GroupProperty parent = groupProperty.getParent();
            if (parent != null) {
                properties[groupProperty.ordinal()] = properties[parent.ordinal()];
                continue;
            }
            properties[groupProperty.ordinal()] = groupProperty.getDefaultValue();
        }
    }

    /**
     * Returns the configured value of a {@link GroupProperty} as String.
     *
     * @param groupProperty the {@link GroupProperty} to get the value from
     * @return the value or <tt>null</tt> if nothing has been configured
     */
    public String getString(GroupProperty groupProperty) {
        return properties[groupProperty.ordinal()];
    }

    /**
     * Returns the configured boolean value of a {@link GroupProperty}.
     *
     * @param groupProperty the {@link GroupProperty} to get the value from
     * @return the value as boolean
     */
    public boolean getBoolean(GroupProperty groupProperty) {
        return Boolean.valueOf(properties[groupProperty.ordinal()]);
    }

    /**
     * Returns the configured int value of a {@link GroupProperty}.
     *
     * @param groupProperty the {@link GroupProperty} to get the value from
     * @return the value as int
     * @throws NumberFormatException if the value cannot be parsed
     */
    public int getInteger(GroupProperty groupProperty) {
        return Integer.parseInt(properties[groupProperty.ordinal()]);
    }

    /**
     * Returns the configured long value of a {@link GroupProperty}.
     *
     * @param groupProperty the {@link GroupProperty} to get the value from
     * @return the value as long
     * @throws NumberFormatException if the value cannot be parsed
     */
    public long getLong(GroupProperty groupProperty) {
        return Long.parseLong(properties[groupProperty.ordinal()]);
    }

    /**
     * Returns the configured float value of a {@link GroupProperty}.
     *
     * @param groupProperty the {@link GroupProperty} to get the value from
     * @return the value as float
     * @throws NumberFormatException if the value cannot be parsed
     */
    public float getFloat(GroupProperty groupProperty) {
        return Float.valueOf(properties[groupProperty.ordinal()]);
    }

    /**
     * Returns the configured value of a {@link GroupProperty} converted to nanoseconds.
     *
     * @param groupProperty the {@link GroupProperty} to get the value from
     * @return the value in nanoseconds
     * @throws IllegalArgumentException if the {@link GroupProperty} has no {@link TimeUnit}
     */
    public long getNanos(GroupProperty groupProperty) {
        TimeUnit timeUnit = groupProperty.getTimeUnit();
        return timeUnit.toNanos(getLong(groupProperty));
    }

    /**
     * Returns the configured value of a {@link GroupProperty} converted to milliseconds.
     *
     * @param groupProperty the {@link GroupProperty} to get the value from
     * @return the value in milliseconds
     * @throws IllegalArgumentException if the {@link GroupProperty} has no {@link TimeUnit}
     */
    public long getMillis(GroupProperty groupProperty) {
        TimeUnit timeUnit = groupProperty.getTimeUnit();
        return timeUnit.toMillis(getLong(groupProperty));
    }

    /**
     * Returns the configured value of a {@link GroupProperty} converted to seconds.
     *
     * @param groupProperty the {@link GroupProperty} to get the value from
     * @return the value in seconds
     * @throws IllegalArgumentException if the {@link GroupProperty} has no {@link TimeUnit}
     */
    public int getSeconds(GroupProperty groupProperty) {
        TimeUnit timeUnit = groupProperty.getTimeUnit();
        return (int) timeUnit.toSeconds(getLong(groupProperty));
    }

    /**
     * Returns the configured enum value of a {@link GroupProperty}.
     * <p/>
     * The case of the enum is ignored.
     *
     * @param groupProperty the {@link GroupProperty} to get the value from
     * @return the enum
     * @throws IllegalArgumentException if the enum value can't be found
     */
    public <E extends Enum> E getEnum(GroupProperty groupProperty, Class<E> enumClazz) {
        String value = getString(groupProperty);

        for (E enumConstant : enumClazz.getEnumConstants()) {
            if (enumConstant.name().equalsIgnoreCase(value)) {
                return enumConstant;
            }
        }

        throw new IllegalArgumentException(format("value '%s' for property '%s' is not a valid %s value",
                value, groupProperty.getName(), enumClazz.getName()));
    }
}
