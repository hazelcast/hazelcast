/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl;

import com.hazelcast.config.IndexConfig;
import com.hazelcast.internal.monitor.impl.PerIndexStats;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.query.impl.getters.Extractors;

/**
 * Provides on-heap indexes.
 */
public class DefaultIndexProvider implements IndexProvider {
    @Override
    public InternalIndex createIndex(
            IndexConfig config,
            Extractors extractors,
            InternalSerializationService ss,
            IndexCopyBehavior copyBehavior,
            PerIndexStats stats,
            int partitionCount) {
        return new IndexImpl(config, ss, extractors, copyBehavior, stats, partitionCount);
    }
}
