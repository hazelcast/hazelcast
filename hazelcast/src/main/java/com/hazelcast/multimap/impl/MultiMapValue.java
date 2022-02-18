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

package com.hazelcast.multimap.impl;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * The MultiMapValue is the value in a multimap and it contains either a set or a list as collection.
 */
public class MultiMapValue {

    private final Collection<MultiMapRecord> collection;

    private long hits;

    public MultiMapValue(Collection<MultiMapRecord> collection) {
        this.collection = collection;
    }

    public Collection<MultiMapRecord> getCollection(boolean copyOf) {
        if (copyOf) {
            return getCopyOfCollection();
        }
        return collection;
    }

    private Collection<MultiMapRecord> getCopyOfCollection() {
        if (collection instanceof Set) {
            return new HashSet<>(collection);
        } else if (collection instanceof List) {
            return new LinkedList<>(collection);
        }
        throw new IllegalArgumentException("No Matching CollectionProxyType!");
    }

    public void incrementHit() {
        hits++;
    }

    public long getHits() {
        return hits;
    }

    public void setHits(long hits) {
        this.hits = hits;
    }

    public boolean containsRecordId(long recordId) {
        for (MultiMapRecord record : collection) {
            if (record.getRecordId() == recordId) {
                return true;
            }
        }
        return false;
    }
}
