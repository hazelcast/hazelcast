/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

/**
 * Contains configuration for Semaphore(read only)
 *
 * @deprecated this class will be removed in 4.0; it is meant for internal usage only.
 */
public class SemaphoreConfigReadOnly extends SemaphoreConfig {

    public SemaphoreConfigReadOnly(SemaphoreConfig config) {
        super(config);
    }

    @Override
    public SemaphoreConfig setName(String name) {
        throw new UnsupportedOperationException("This config is read-only semaphore: " + getName());
    }

    @Override
    public SemaphoreConfig setInitialPermits(int initialPermits) {
        throw new UnsupportedOperationException("This config is read-only semaphore: " + getName());
    }

    @Override
    public SemaphoreConfig setBackupCount(int backupCount) {
        throw new UnsupportedOperationException("This config is read-only semaphore: " + getName());
    }

    @Override
    public SemaphoreConfig setAsyncBackupCount(int asyncBackupCount) {
        throw new UnsupportedOperationException("This config is read-only semaphore: " + getName());
    }

    @Override
    public SemaphoreConfig setQuorumName(String quorumName) {
        throw new UnsupportedOperationException("This config is read-only semaphore: " + getName());
    }
}
