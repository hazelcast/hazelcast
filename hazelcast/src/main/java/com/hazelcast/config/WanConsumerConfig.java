/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import java.util.HashMap;
import java.util.Map;

/**
 * Config to be used by WanReplicationConsumer instances. (EE only)
 */
public class WanConsumerConfig {

    private Map<String, Comparable> properties = new HashMap<String, Comparable>();
    private String className;
    private Object implementation;

    public Map<String, Comparable> getProperties() {
        return properties;
    }

    public String getClassName() {
        return className;
    }

    public WanConsumerConfig setClassName(String className) {
        this.className = className;
        return this;
    }

    public Object getImplementation() {
        return implementation;
    }

    public WanConsumerConfig setImplementation(Object implementation) {
        this.implementation = implementation;
        return this;
    }

    public WanConsumerConfig setProperties(Map<String, Comparable> properties) {
        this.properties = properties;
        return this;
    }

    @Override
    public String toString() {
        return "WanConsumerConfig{"
                + "properties=" + properties
                + ", className='" + className + '\''
                + ", implementation=" + implementation
                + '}';
    }
}
