/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import com.hazelcast.core.QueueStore;

import java.util.Properties;

/**
 * @ali 12/14/12
 */
public class QueueStoreConfig {

    public static final int DEFAULT_WRITE_DELAY_SECONDS = 0;

    private boolean enabled = true;
    private String className = null;
    private int writeDelaySeconds = DEFAULT_WRITE_DELAY_SECONDS;
    private Properties properties = new Properties();
    private QueueStore storeImplementation;

    public QueueStoreConfig() {
    }

    public QueueStoreConfig(String className, boolean enabled) {
        this.className = className;
        this.enabled = enabled;
    }

    public QueueStoreConfig(QueueStore storeImplementation) {
        this.storeImplementation = storeImplementation;
    }

    public QueueStore getStoreImplementation() {
        return storeImplementation;
    }

    public QueueStoreConfig setStoreImplementation(QueueStore storeImplementation) {
        this.storeImplementation = storeImplementation;
        return this;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public QueueStoreConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    public String getClassName() {
        return className;
    }

    public QueueStoreConfig setClassName(String className) {
        this.className = className;
        return this;
    }

    public int getWriteDelaySeconds() {
        return writeDelaySeconds;
    }

    public QueueStoreConfig setWriteDelaySeconds(int writeDelaySeconds) {
        this.writeDelaySeconds = writeDelaySeconds;
        return this;
    }

    public Properties getProperties() {
        return properties;
    }

    public QueueStoreConfig setProperties(Properties properties) {
        this.properties = properties;
        return this;
    }

    public String getProperty(String name){
        return properties.getProperty(name);
    }

    public QueueStoreConfig setProperty(String name, String value){
        properties.put(name, value);
        return this;
    }

    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("QueueStoreConfig");
        sb.append("{enabled=").append(enabled);
        sb.append(", className='").append(className).append('\'');
        sb.append(", writeDelaySeconds=").append(writeDelaySeconds);
        sb.append(", properties=").append(properties);
        sb.append('}');
        return sb.toString();
    }
}
