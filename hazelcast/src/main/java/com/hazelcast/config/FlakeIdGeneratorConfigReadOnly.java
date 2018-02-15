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
 * See {@link FlakeIdGeneratorConfig}
 *
 * @deprecated this class will be removed in 4.0; it is meant for internal usage only.
 */
public class FlakeIdGeneratorConfigReadOnly extends FlakeIdGeneratorConfig {

    FlakeIdGeneratorConfigReadOnly(FlakeIdGeneratorConfig config) {
        super(config);
    }

    @Override
    public void setName(String name) {
        throw throwReadOnly();
    }

    @Override
    public FlakeIdGeneratorConfig setPrefetchCount(int prefetchCount) {
        throw throwReadOnly();
    }

    @Override
    public FlakeIdGeneratorConfig setPrefetchValidityMillis(long prefetchValidityMs) {
        throw throwReadOnly();
    }

    @Override
    public FlakeIdGeneratorConfig setIdOffset(long idOffset) {
        throw throwReadOnly();
    }

    @Override
    public FlakeIdGeneratorConfig setStatisticsEnabled(boolean statisticsEnabled) {
        throw throwReadOnly();
    }

    private UnsupportedOperationException throwReadOnly() {
        throw new UnsupportedOperationException("This config is read-only");
    }
}
