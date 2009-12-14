/* 
 * Copyright (c) 2008-2009, Hazel Ltd. All Rights Reserved.
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

package com.hazelcast.config;

public enum ConfigProperty {
    SERIALIZER_SHARED("hazelcast.serializer.shared", "false"),
    SHUTDOWNHOOK_ENABLED("hazelcast.shutdownhook.enabled", "true"),
    WAIT_SECONDS_BEFORE_JOIN("hazelcast.wait.seconds.before.join", "5"),
    MAX_NO_HEARTBEAT_SECONDS("hazelcast.max.no.heartbeat.seconds", "30"),
    CONCURRENT_MAP_BLOCK_COUNT("hazelcast.map.block.count", "271"),
    BLOCKING_QUEUE_BLOCK_SIZE("hazelcast.queue.block.size", "1000"),
    REMOVE_DELAY_SECONDS("hazelcast.map.remove.delay.seconds", "5"),
    PACKET_VERSION("hazelcast.packet.version", "1");

    private final String name;
    private final String defaultValue;

    ConfigProperty(String name, String defaultValue) {
        this.name = name;
        this.defaultValue = defaultValue;
    }

    public String getName() {
        return this.name;
    }

    public int getInteger() {
        return Integer.getInteger(name, Integer.parseInt(this.defaultValue));
    }

    public int getByte() {
        return getByte(Byte.parseByte(this.defaultValue));
    }

    public boolean getBoolean() {
        String val = System.getProperty(name);
        if (val == null) return Boolean.valueOf(this.defaultValue);
        return ("true".equalsIgnoreCase(val));
    }

    public String getString() {
        String val = System.getProperty(name);
        if (val == null) return this.defaultValue;
        return val;
    }

    public long getLong() {
        return Long.getLong(name, Long.parseLong(this.defaultValue));
    }

    public int getInteger(int defaultValue) {
        return Integer.getInteger(name, defaultValue);
    }

    public byte getByte(byte defaultValue) {
        String val = System.getProperty(name);
        if (val == null) return defaultValue;
        return Byte.parseByte(val);
    }

    public boolean getBoolean(boolean defaultValue) {
        String val = System.getProperty(name);
        if (val == null) return defaultValue;
        return ("true".equalsIgnoreCase(val));
    }

    public String getString(String defaultValue) {
        String val = System.getProperty(name);
        if (val == null) return defaultValue;
        return val;
    }

    public long getLong(long defaultValue) {
        return Long.getLong(name, defaultValue);
    }

    public String getValue() {
        return System.getProperty(name);
    }
}
