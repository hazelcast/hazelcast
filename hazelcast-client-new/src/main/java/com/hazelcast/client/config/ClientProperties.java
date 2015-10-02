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

import com.hazelcast.config.Config;
import com.hazelcast.instance.HazelcastProperties;

import static com.hazelcast.client.config.ClientProperty.EVENT_QUEUE_CAPACITY;
import static com.hazelcast.client.config.ClientProperty.EVENT_THREAD_COUNT;
import static com.hazelcast.client.config.ClientProperty.HEARTBEAT_INTERVAL;
import static com.hazelcast.client.config.ClientProperty.HEARTBEAT_TIMEOUT;
import static com.hazelcast.client.config.ClientProperty.INVOCATION_TIMEOUT_SECONDS;
import static com.hazelcast.client.config.ClientProperty.SHUFFLE_MEMBER_LIST;

/**
 * Container for configured Hazelcast Client properties ({@see ClientProperty}).
 * <p/>
 * A {@link ClientProperty} can be set as:
 * <p><ul>
 * <li>an environmental variable using {@link System#setProperty(String, String)}</li>
 * <li>the programmatic configuration using {@link Config#setProperty(String, String)}</li>
 * <li>the XML configuration
 * {@see http://docs.hazelcast.org/docs/latest-dev/manual/html-single/hazelcast-documentation.html#system-properties}</li>
 * </ul></p>
 * The old property definitions are deprecated since Hazelcast 3.6. Please use the new {@link ClientProperty} definitions instead.
 */
@SuppressWarnings("unused")
public class ClientProperties extends HazelcastProperties {

    /**
     * Deprecated since Hazelcast 3.6. Please use the new {@link ClientProperty#SHUFFLE_MEMBER_LIST} instead.
     */
    @Deprecated
    public static final String PROP_SHUFFLE_MEMBER_LIST = SHUFFLE_MEMBER_LIST.getName();

    /**
     * Deprecated since Hazelcast 3.6. Please use the new {@link ClientProperty#SHUFFLE_MEMBER_LIST} instead.
     */
    @Deprecated
    public static final String PROP_SHUFFLE_INITIAL_MEMBER_LIST_DEFAULT = SHUFFLE_MEMBER_LIST.getDefaultValue();

    /**
     * Deprecated since Hazelcast 3.6. Please use the new {@link ClientProperty#HEARTBEAT_TIMEOUT} instead.
     */
    @Deprecated
    public static final String PROP_HEARTBEAT_TIMEOUT = HEARTBEAT_TIMEOUT.getName();

    /**
     * Deprecated since Hazelcast 3.6. Please use the new {@link ClientProperty#HEARTBEAT_TIMEOUT} instead.
     */
    @Deprecated
    public static final String PROP_HEARTBEAT_TIMEOUT_DEFAULT = HEARTBEAT_TIMEOUT.getDefaultValue();

    /**
     * Deprecated since Hazelcast 3.6. Please use the new {@link ClientProperty#HEARTBEAT_INTERVAL} instead.
     */
    @Deprecated
    public static final String PROP_HEARTBEAT_INTERVAL = HEARTBEAT_INTERVAL.getName();

    /**
     * Deprecated since Hazelcast 3.6. Please use the new {@link ClientProperty#HEARTBEAT_INTERVAL} instead.
     */
    @Deprecated
    public static final String PROP_HEARTBEAT_INTERVAL_DEFAULT = HEARTBEAT_INTERVAL.getDefaultValue();

    /**
     * Deprecated since Hazelcast 3.6. Please use the new {@link ClientProperty#EVENT_THREAD_COUNT} instead.
     */
    @Deprecated
    public static final String PROP_EVENT_THREAD_COUNT = EVENT_THREAD_COUNT.getName();

    /**
     * Deprecated since Hazelcast 3.6. Please use the new {@link ClientProperty#EVENT_THREAD_COUNT} instead.
     */
    @Deprecated
    public static final String PROP_EVENT_THREAD_COUNT_DEFAULT = EVENT_THREAD_COUNT.getDefaultValue();

    /**
     * Deprecated since Hazelcast 3.6. Please use the new {@link ClientProperty#EVENT_QUEUE_CAPACITY} instead.
     */
    @Deprecated
    public static final String PROP_EVENT_QUEUE_CAPACITY = EVENT_QUEUE_CAPACITY.getName();

    /**
     * Deprecated since Hazelcast 3.6. Please use the new {@link ClientProperty#EVENT_QUEUE_CAPACITY} instead.
     */
    @Deprecated
    public static final String PROP_EVENT_QUEUE_CAPACITY_DEFAULT = EVENT_QUEUE_CAPACITY.getDefaultValue();

    /**
     * Deprecated since Hazelcast 3.6. Please use the new {@link ClientProperty#INVOCATION_TIMEOUT_SECONDS} instead.
     */
    @Deprecated
    public static final String PROP_INVOCATION_TIMEOUT_SECONDS = INVOCATION_TIMEOUT_SECONDS.getName();

    /**
     * Deprecated since Hazelcast 3.6. Please use the new {@link ClientProperty#INVOCATION_TIMEOUT_SECONDS} instead.
     */
    @Deprecated
    public static final String PROP_INVOCATION_TIMEOUT_SECONDS_DEFAULT = INVOCATION_TIMEOUT_SECONDS.getDefaultValue();

    /**
     * Creates a container with configured Hazelcast properties.
     * <p/>
     * Uses the environmental value if no value is defined in the configuration.
     * Uses the default value if no environmental value is defined.
     *
     * @param config {@link Config} used to configure the {@link ClientProperty} values.
     */
    public ClientProperties(ClientConfig config) {
        initProperties(config.getProperties(), ClientProperty.values());
    }

    @Override
    protected String[] createProperties() {
        return new String[ClientProperty.values().length];
    }
}
