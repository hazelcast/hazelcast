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

package com.hazelcast.config;

import java.util.Properties;
/**
 * Contains the configuration for a Map Store (read-only).
 */
public class MapStoreConfigReadOnly extends MapStoreConfig {

    public MapStoreConfigReadOnly(MapStoreConfig config) {
        super(config);
    }

    public MapStoreConfig setClassName(String className) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    public MapStoreConfig setFactoryClassName(String factoryClassName) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    public MapStoreConfig setWriteDelaySeconds(int writeDelaySeconds) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    public MapStoreConfig setWriteBatchSize(int writeBatchSize) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    public MapStoreConfig setEnabled(boolean enabled) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    public MapStoreConfig setImplementation(Object implementation) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    public MapStoreConfig setInitialLoadMode(InitialLoadMode initialLoadMode) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    public MapStoreConfig setFactoryImplementation(Object factoryImplementation) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    public MapStoreConfig setProperty(String name, String value) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    public MapStoreConfig setProperties(Properties properties) {
        throw new UnsupportedOperationException("This config is read-only");
    }
}
