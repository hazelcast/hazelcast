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

package com.hazelcast.multimap.impl;

import static com.hazelcast.multimap.impl.ValueCollectionFactory.createCollection;
import static com.hazelcast.util.MapUtil.createConcurrentHashMap;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

/**
 * Contains various {@link com.hazelcast.multimap.impl.MultiMapContainer} support methods.
 */
abstract class MultiMapContainerSupport {

    protected final ConcurrentMap<Data, MultiMapValue> multiMapValues = createConcurrentHashMap(1000);
    protected volatile int size;

    protected final String name;
    protected final NodeEngine nodeEngine;
    protected final MultiMapConfig config;

    MultiMapContainerSupport(String name, NodeEngine nodeEngine) {
        this.name = name;
        this.nodeEngine = nodeEngine;
        this.config = nodeEngine.getConfig().findMultiMapConfig(name);
    }

    public MultiMapValue getOrCreateMultiMapValue(Data dataKey) {
        MultiMapValue multiMapValue = multiMapValues.get(dataKey);
        if (multiMapValue != null) {
            return multiMapValue;
        }
        // create multiMapValue
        final Collection<MultiMapRecord> collection = getNewRecordsCollection();
        multiMapValue = new MultiMapValue(collection);

        multiMapValues.put(dataKey, multiMapValue);

        return multiMapValue;
    }

    protected Collection<MultiMapRecord> getNewRecordsCollection() {
        final MultiMapConfig.ValueCollectionType valueCollectionType = config.getValueCollectionType();
        return createCollection(valueCollectionType);
    }

    public MultiMapValue getMultiMapValueOrNull(Data dataKey) {
        return multiMapValues.get(dataKey);
    }

    public ConcurrentMap<Data, MultiMapValue> getMultiMapValues() {
        return multiMapValues;
    }

    public void setValue(Data dataKey, MultiMapValue value) {
        MultiMapValue prevValue = multiMapValues.put(dataKey, value);
        if (prevValue != null) {
            decrSize(prevValue.size());
        }
        incSize(value.size());
    }

    public boolean setValue(Data dataKey, Collection<MultiMapRecord> records) {
        MultiMapValue multiMapValue = getOrCreateMultiMapValue(dataKey);
        Collection<MultiMapRecord> mapRecords = multiMapValue.getCollection(false);
        decrSize(mapRecords.size());
        mapRecords.clear();

        boolean added = mapRecords.addAll(records);
        if (added) {
            incSize(records.size());
        }
        return added;
    }

    public boolean addValue(Data dataKey, MultiMapRecord value) {
        return addValue(dataKey, value, -1);
    }

    public boolean addValue(Data dataKey, MultiMapRecord value, int index) {
        MultiMapValue multiMapValue = getOrCreateMultiMapValue(dataKey);
        boolean added;
        if (index < 0) {
            added = multiMapValue.getCollection(false).add(value);
        } else {
            ((List<MultiMapRecord>) multiMapValue.getCollection(false)).add(index, value);
            added = true;
        }
        if (added) {
            incSize(1);
        }
        return added;
    }

    public boolean removeValue(Data dataKey, long recordId) {
        MultiMapValue multiMapValue = getMultiMapValueOrNull(dataKey);
        if (multiMapValue == null) {
            return false;
        }
        Collection<MultiMapRecord> coll = multiMapValue.getCollection(false);
        Iterator<MultiMapRecord> iterator = coll.iterator();
        while (iterator.hasNext()) {
            if (iterator.next().getRecordId() == recordId) {
                iterator.remove();
                decrSize(1);
                if (coll.isEmpty()) {
                    multiMapValues.remove(dataKey);
                }
                return true;
            }
        }
        return false;
    }

    public long removeValue(Data dataKey, MultiMapRecord record) {
        MultiMapValue multiMapValue = getMultiMapValueOrNull(dataKey);
        if (multiMapValue == null) {
            return -1;
        }
        Collection<MultiMapRecord> coll = multiMapValue.getCollection(false);
        Iterator<MultiMapRecord> iterator = coll.iterator();
        while (iterator.hasNext()) {
            MultiMapRecord multiMapRecord = iterator.next();
            if (multiMapRecord.equals(record)) {
                iterator.remove();
                decrSize(1);
                if (coll.isEmpty()) {
                    multiMapValues.remove(dataKey);
                }
                return multiMapRecord.getRecordId();
            }
        }
        return -1;
    }

    public List<MultiMapRecord> removeAllValues(Data dataKey, Collection<Long> recordIds) {
        List<MultiMapRecord> removed = new LinkedList<MultiMapRecord>();
        MultiMapValue multiMapValue = getOrCreateMultiMapValue(dataKey);
        for (Long recordId : recordIds) {
            if (!multiMapValue.containsRecordId(recordId)) {
                return removed;
            }
        }

        int numOfRecsRemoved = 0;
        Collection<MultiMapRecord> coll = multiMapValue.getCollection(false);
        for (Long recordId : recordIds) {
            Iterator<MultiMapRecord> iterator = coll.iterator();
            while (iterator.hasNext()) {
                MultiMapRecord record = iterator.next();
                if (record.getRecordId() == recordId) {
                    iterator.remove();
                    removed.add(record);
                    ++numOfRecsRemoved;
                    break;
                }
            }
        }
        decrSize(numOfRecsRemoved);
        if (coll.isEmpty()) {
            multiMapValues.remove(dataKey);
        }
        return removed;
    }

    public boolean delete(Data dataKey) {
        MultiMapValue value = multiMapValues.remove(dataKey);
        if (value != null) {
            decrSize(value.size());
            return true;
        } else {
            return false;
        }
    }

    public Collection<MultiMapRecord> remove(Data dataKey, boolean copyOf) {
        MultiMapValue multiMapValue = multiMapValues.remove(dataKey);
        if (multiMapValue != null) {
            decrSize(multiMapValue.size());
            return multiMapValue.getCollection(copyOf);
        } else {
            return null;
        }
    }

    public MultiMapConfig getConfig() {
        return config;
    }

    public void incSize(int by) {
        size += by;
    }

    public void decrSize(int by) {
        size -= by;
    }
}
