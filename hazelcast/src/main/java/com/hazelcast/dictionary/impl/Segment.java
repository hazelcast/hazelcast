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

package com.hazelcast.dictionary.impl;

import com.hazelcast.config.DictionaryConfig;
import com.hazelcast.dictionary.impl.type.EntryType;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;

import java.util.concurrent.atomic.AtomicReference;


/**
 * The Segment is created eagerly as soon as the partition for the dictionary
 * is created, but the memory is allocated lazily.
 */
public class Segment {

    private final AtomicReference<SegmentTask> ref = new AtomicReference<>();
    private final SerializationService serializationService;

    private OffsetRegion offsetRegion;
    private DataRegion dataRegion;
    private volatile boolean allocated;
    private final boolean isFixedLengthValue;

    public Segment(SerializationService serializationService,
                   EntryType entryType,
                   EntryEncoder encoder,
                   DictionaryConfig config) {
        this.serializationService = serializationService;
        this.dataRegion = new DataRegion(config, encoder, entryType);
        this.offsetRegion = new OffsetRegion(config.getInitialSegmentSize(), dataRegion, encoder);
        this.isFixedLengthValue = entryType.value().isFixedLength();
    }

    /**
     * Returns the number of bytes allocated in this Segment.
     *
     * @return number of bytes allocated.
     */
    public long allocated() {
        return offsetRegion.allocated() + dataRegion.allocated();
    }

    /**
     * Returns the number of bytes consumed in the segment.
     *
     * @return number of bytes consumed.
     */
    public long consumed() {
        return dataRegion.consumed() + offsetRegion.consumed();
    }

    public boolean isAllocated() {
        return allocated;
    }

    private void ensureAllocated() {
        if (!allocated) {
            allocated = true;
            dataRegion.init();
            offsetRegion.init();
        }
    }

    /**
     * Executes the task on this segment.
     *
     * The task is executed immediately if the segment is available, or parked for later
     * execution when the segment is in use.
     *
     * @param task
     * @return true if the task got executed, false if the task is appended for later execution.
     */
    public boolean execute(SegmentTask task) {
        return false;
    }

    public void clear() {
        dataRegion.clear();
        offsetRegion.clear();
    }

    /**
     * Returns the number of entries in this Segment.
     *
     * This method is thread-safe.
     *
     * @return the number of entries in this Segment.
     */
    public int count() {
        return dataRegion.count();
    }

    public boolean put(Data keyData, int partitionHash, Data valueData, boolean overwrite) {
        ensureAllocated();

        // creating these objects can cause performance problems. E.g. when the value is a large
        // byte array. So we should not be forcing to pull these objects to memory.
        Object key = serializationService.toObject(keyData);
        Object value = serializationService.toObject(valueData);

        int offset = offsetRegion.search(key, partitionHash);

        if (offset == -1) {
            //System.out.println("previous value not found, inserting");
            offset = dataRegion.insert(key, value);
            offsetRegion.insert(key, partitionHash, offset);
        } else {
            if (!overwrite) {
                return false;
            }

            //System.out.println("previous value found, overwriting");
            // todo: we assume that we can overwrite; but with variable length records this doesn't need to be the case
            dataRegion.overwrite(value, offset);
        }

        return true;
    }

    public Object get(Data keyData, int partitionHash) {
        if (!allocated) {
            // no memory has been allocated, so no items are stored.
            return null;
        }

        Object key = serializationService.toObject(keyData);
        int offset = offsetRegion.search(key, partitionHash);
        return offset == -1 ? null : dataRegion.readValue(offset);
    }

    public boolean containsKey(Data keyData, int partitionHash) {
        if (!allocated) {
            // no memory has been allocated, so no items are stored.
            return false;
        }

        Object key = serializationService.toObject(keyData);
        int offset = offsetRegion.search(key, partitionHash);
        return offset != -1;
    }

    public boolean replace(Data keyData, int partitionHash, Data updatedValueData) {
        if (!allocated) {
            return false;
        }

        // creating these objects can cause performance problems. E.g. when the updatedValue is a large
        // byte array. So we should not be forcing to pull these objects to memory.
        Object key = serializationService.toObject(keyData);

        if (isFixedLengthValue) {
            // fixed length value can be overwritten
            int offset = offsetRegion.search(key, partitionHash);

            if (offset == -1) {
                return false;
            }

            Object updatedValue = serializationService.toObject(updatedValueData);
            dataRegion.overwrite(updatedValue, offset);
        } else {
            // in case of variable length value, the entry is removed and re-inserted.
            int offset = offsetRegion.remove(key, partitionHash);

            if (offset == -1) {
                return false;
            }

            dataRegion.remove(offset);

            Object updatedValue = serializationService.toObject(updatedValueData);
            offset = dataRegion.insert(key, updatedValue);
            offsetRegion.insert(key, partitionHash, offset);
        }

        return true;
    }

    public Object getAndReplace(Data keyData, int partitionHash, Data valueData) {
        if (!allocated) {
            return null;
        }

        // creating these objects can cause performance problems. E.g. when the value is a large
        // byte array. So we should not be forcing to pull these objects to memory.
        Object key = serializationService.toObject(keyData);

        int offset = offsetRegion.search(key, partitionHash);

        if (offset == -1) {
            return null;
        }

        Object value = serializationService.toObject(valueData);

        Object oldValue = dataRegion.readValue(offset);

        dataRegion.overwrite(value, offset);

        return oldValue;
    }

    public boolean remove(Data keyData, int partitionHash) {
        if (!allocated) {
            return false;
        }

        // creating these objects can cause performance problems. E.g. when the value is a large
        // byte array. So we should not be forcing to pull these objects to memory.
        Object key = serializationService.toObject(keyData);

        int offset = offsetRegion.remove(key, partitionHash);

        if (offset == -1) {
            return false;
        }

        dataRegion.remove(offset);

        return true;
    }
}
