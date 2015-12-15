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

import com.hazelcast.instance.GroupProperty;
import com.hazelcast.instance.HazelcastProperty;
import com.hazelcast.spi.annotation.PrivateApi;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.Preconditions.checkHasText;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Defines the name and default value for Hazelcast Client properties.
 */
@PrivateApi
public enum ClientProperty implements HazelcastProperty {

    /**
     * Client shuffles the given member list to prevent all clients to connect to the same node when
     * this property is set to false. When it is set to true, the client tries to connect to the nodes
     * in the given order.
     */
    SHUFFLE_MEMBER_LIST("hazelcast.client.shuffle.member.list", true),

    /**
     * Client sends heartbeat messages to the members and this is the timeout for this sending operations.
     * If there is not any message passing between the client and member within the given time via this property
     * in milliseconds, the connection will be closed.
     */
    HEARTBEAT_TIMEOUT("hazelcast.client.heartbeat.timeout", 60000, MILLISECONDS),

    /**
     * Time interval between the heartbeats sent by the client to the nodes.
     */
    HEARTBEAT_INTERVAL("hazelcast.client.heartbeat.interval", 5000, MILLISECONDS),

    /**
     * Number of the threads to handle the incoming event packets.
     */
    EVENT_THREAD_COUNT("hazelcast.client.event.thread.count", 5),

    /**
     * Capacity of the executor that handles the incoming event packets.
     */
    EVENT_QUEUE_CAPACITY("hazelcast.client.event.queue.capacity", 1000000),

    /**
     * Time to give up on invocation when a member in the member list is not reachable.
     */
    INVOCATION_TIMEOUT_SECONDS("hazelcast.client.invocation.timeout.seconds", 120, SECONDS),

    /**
     * The maximum number of concurrent invocations allowed.
     * <p/>
     * To prevent the system from overloading, user can apply a constraint on the number of concurrent invocations.
     * If the maximum number of concurrent invocations has been exceeded and a new invocation comes in,
     * then hazelcast will throw HazelcastOverloadException
     * <p/>
     * By default it is configured as Integer.MaxValue.
     */
    MAX_CONCURRENT_INVOCATIONS("hazelcast.client.max.concurrent.invocations", Integer.MAX_VALUE);

    private final String name;
    private final String defaultValue;
    private final TimeUnit timeUnit;

    ClientProperty(String name, boolean defaultValue) {
        this(name, defaultValue ? "true" : "false");
    }

    ClientProperty(String name, Integer defaultValue) {
        this(name, String.valueOf(defaultValue));
    }

    ClientProperty(String name, String defaultValue) {
        this(name, defaultValue, null);
    }

    ClientProperty(String name, Integer defaultValue, TimeUnit timeUnit) {
        this(name, String.valueOf(defaultValue), timeUnit);
    }

    ClientProperty(String name, String defaultValue, TimeUnit timeUnit) {
        checkHasText(name, "The property name cannot be null or empty!");

        this.name = name;
        this.defaultValue = defaultValue;
        this.timeUnit = timeUnit;
    }

    @Override
    public int getIndex() {
        return ordinal();
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public String getDefaultValue() {
        return defaultValue;
    }

    @Override
    public TimeUnit getTimeUnit() {
        if (timeUnit == null) {
            throw new IllegalArgumentException(format("groupProperty %s has no TimeUnit defined!", this));
        }
        return timeUnit;
    }

    @Override
    public GroupProperty getParent() {
        return null;
    }

    @Override
    public void setSystemProperty(String value) {
        System.setProperty(name, value);
    }

    @Override
    public String getSystemProperty() {
        return System.getProperty(name);
    }

    @Override
    public String clearSystemProperty() {
        return System.clearProperty(name);
    }

    @Override
    public String toString() {
        return name;
    }
}
