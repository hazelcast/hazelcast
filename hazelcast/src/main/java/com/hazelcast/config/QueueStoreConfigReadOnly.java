/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.QueueStoreFactory;

import java.util.Properties;

/**
 * Contains the configuration for an {@link com.hazelcast.core.QueueStore}.
 *
 * @deprecated this class will be removed in 4.0; it is meant for internal usage only.
 */
public class QueueStoreConfigReadOnly extends QueueStoreConfig {

    public QueueStoreConfigReadOnly(QueueStoreConfig config) {
        super(config);
    }

    @Override
    public QueueStoreConfig setStoreImplementation(QueueStore storeImplementation) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public QueueStoreConfig setEnabled(boolean enabled) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public QueueStoreConfig setClassName(String className) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public QueueStoreConfig setProperties(Properties properties) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public QueueStoreConfig setProperty(String name, String value) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public QueueStoreConfig setFactoryClassName(String factoryClassName) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public QueueStoreConfig setFactoryImplementation(QueueStoreFactory factoryImplementation) {
        throw new UnsupportedOperationException("This config is read-only");
    }
}
