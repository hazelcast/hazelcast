/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl.merge.policy;

import com.hazelcast.cache.impl.merge.entry.CacheEntryView;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * `PassThroughCacheMergePolicy` policy merges cache entry to from source to destination
 * if it does not exist in the destination cache.
 */
public class PutIfAbsentCacheMergePolicy implements CacheMergePolicy {

    @Override
    public Object merge(String cacheName, CacheEntryView mergingEntry, CacheEntryView existingEntry) {
        if (existingEntry == null) {
            return mergingEntry.getValue();
        }
        return existingEntry.getValue();
    }

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
    }

}
