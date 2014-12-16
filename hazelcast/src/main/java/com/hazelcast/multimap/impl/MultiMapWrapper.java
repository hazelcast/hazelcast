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

package com.hazelcast.multimap.impl;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class MultiMapWrapper {

    private final Collection<MultiMapRecord> collection;

    private int hits;

    public MultiMapWrapper(Collection<MultiMapRecord> collection) {
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
            return new HashSet<MultiMapRecord>(collection);
        } else if (collection instanceof List) {
            return new LinkedList<MultiMapRecord>(collection);
        }
        throw new IllegalArgumentException("No Matching CollectionProxyType!");
    }

    public void incrementHit() {
        hits++;
    }

    public int getHits() {
        return hits;
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
