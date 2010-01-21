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

    public static final GroupProperty SERIALIZER_SHARED = new GroupProperty(null, "hazelcast.serializer.shared", "false");

    public static final GroupProperty PACKET_VERSION = new GroupProperty(null, "hazelcast.packet.version", "3");

    public final GroupProperty SHUTDOWNHOOK_ENABLED;

    public final GroupProperty WAIT_SECONDS_BEFORE_JOIN;

    public final GroupProperty MAX_NO_HEARTBEAT_SECONDS;

    public final GroupProperty FIRST_MEMBER_WAIT_SECONDS;

    public final GroupProperty RESTART_ON_MAX_IDLE;

    public final GroupProperty CONCURRENT_MAP_BLOCK_COUNT;

    public final GroupProperty BLOCKING_QUEUE_BLOCK_SIZE;

    public final GroupProperty REMOVE_DELAY_SECONDS;

    public final GroupProperty LOG_STATE;

    public GroupProperties(Config config) {
        SHUTDOWNHOOK_ENABLED = new GroupProperty(config, "hazelcast.shutdownhook.enabled", "true");
        WAIT_SECONDS_BEFORE_JOIN = new GroupProperty(config, "hazelcast.wait.seconds.before.join", "5");
        MAX_NO_HEARTBEAT_SECONDS = new GroupProperty(config, "hazelcast.max.no.heartbeat.seconds", "300");
        FIRST_MEMBER_WAIT_SECONDS = new GroupProperty(config, "hazelcast.first.member.wait.seconds", "0");
        RESTART_ON_MAX_IDLE = new GroupProperty(config, "hazelcast.restart.on.max.idle", "false");
        CONCURRENT_MAP_BLOCK_COUNT = new GroupProperty(config, "hazelcast.map.block.count", "271");
        BLOCKING_QUEUE_BLOCK_SIZE = new GroupProperty(config, "hazelcast.queue.block.size", "1000");
        REMOVE_DELAY_SECONDS = new GroupProperty(config, "hazelcast.map.remove.delay.seconds", "5");
        LOG_STATE = new GroupProperty(config, "hazelcast.log.state", "false");
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
