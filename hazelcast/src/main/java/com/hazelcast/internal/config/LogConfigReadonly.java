/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.config;

import com.hazelcast.config.LogConfig;
import com.hazelcast.log.encoders.Encoder;

import java.util.concurrent.TimeUnit;

public class LogConfigReadonly extends LogConfig {
    public LogConfigReadonly(LogConfig config) {
        super(config);
    }

    @Override
    public LogConfig setBackupCount(int backupCount) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public LogConfig setAsyncBackupCount(int asyncBackupCount) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public LogConfig setTenuringAge(long age, TimeUnit unit) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public LogConfig setTenuringAgeMillis(long tenuringAgeMillis) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public LogConfig setRetentionMillis(long retentionPeriod, TimeUnit unit) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public LogConfig setRetentionMillis(long retentionMillis) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public LogConfig setType(Class clazz) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public LogConfig setType(String type) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public LogConfig setSegmentSize(int segmentSize) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public LogConfig setMaxSegmentCount(int maxSegmentCount) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public LogConfig setEncoder(Encoder encoder) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public LogConfig setName(String name) {
        throw new UnsupportedOperationException("This config is read-only");
    }
}
