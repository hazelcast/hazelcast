/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.hibernate;

import org.hibernate.cache.entry.CacheEntry;

import com.hazelcast.core.MapEntry;
import com.hazelcast.impl.base.DataRecordEntry;
import com.hazelcast.merge.MergePolicy;

public class VersionAwareMergePolicy implements MergePolicy {
    public static final String NAME = "hz.HIBERNATE_VERSION_AWARE";

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public Object merge(String mapName, MapEntry mergingEntry, MapEntry existingEntry) {
        DataRecordEntry mergingDataEntry = (DataRecordEntry) mergingEntry;
        if (!mergingDataEntry.isValid()) {
            return REMOVE_EXISTING; 
        } else {
            final Object existingObject = existingEntry != null ? existingEntry.getValue() : null;
            final Object mergingObject = mergingEntry.getValue();
            if (existingObject != null && existingObject instanceof CacheEntry 
                    && mergingObject != null && mergingObject instanceof CacheEntry) {
                final CacheEntry existing = (CacheEntry) existingObject;
                final CacheEntry merging = (CacheEntry) mergingObject;
                final Object mergingVersionObject = merging.getVersion();
                final Object existingVersionObject = existing.getVersion();
                if (mergingVersionObject != null && existingVersionObject != null
                        && mergingVersionObject instanceof Comparable && existingVersionObject instanceof Comparable) {
                    final Comparable mergingVersion = (Comparable) mergingVersionObject;
                    final Comparable existingVersion = (Comparable) existingVersionObject;
                    if (mergingVersion.compareTo(existingVersion) > 0) {
                        return mergingDataEntry.getValueData();
                    } else {
                        return ((DataRecordEntry) existingEntry).getValueData();
                    }
                }
            }
            return mergingDataEntry.getValueData();
        }
    }
}
