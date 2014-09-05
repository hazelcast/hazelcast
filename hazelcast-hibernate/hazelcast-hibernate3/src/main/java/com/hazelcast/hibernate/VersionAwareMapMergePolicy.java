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

package com.hazelcast.hibernate;

import com.hazelcast.core.EntryView;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import org.hibernate.cache.entry.CacheEntry;

import java.io.IOException;

/**
 *  A merge policy implementation to handle split brain remerges based on the timestamps stored in
 *  the values.
 */
public class VersionAwareMapMergePolicy implements MapMergePolicy {

    public Object merge(String mapName, EntryView mergingEntry, EntryView existingEntry) {
        final Object existingValue = existingEntry != null ? existingEntry.getValue() : null;
        final Object mergingValue = mergingEntry.getValue();
        if (existingValue != null && existingValue instanceof CacheEntry
                && mergingValue != null && mergingValue instanceof CacheEntry) {

            final CacheEntry existingCacheEntry = (CacheEntry) existingValue;
            final CacheEntry mergingCacheEntry = (CacheEntry) mergingValue;
            final Object mergingVersionObject = mergingCacheEntry.getVersion();
            final Object existingVersionObject = existingCacheEntry.getVersion();
            if (mergingVersionObject != null && existingVersionObject != null
                    && mergingVersionObject instanceof Comparable && existingVersionObject instanceof Comparable) {

                final Comparable mergingVersion = (Comparable) mergingVersionObject;
                final Comparable existingVersion = (Comparable) existingVersionObject;

                if (mergingVersion.compareTo(existingVersion) > 0) {
                    return mergingValue;
                } else {
                    return existingValue;
                }
            }
        }
        return mergingValue;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
    }
}
