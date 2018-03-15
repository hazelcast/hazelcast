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
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;

import java.util.concurrent.atomic.AtomicReference;


/**
 * http://www.docjar.com/docs/api/sun/misc/Unsafe.html
 *
 * http://hg.openjdk.java.net/jdk8/jdk8/jdk/file/687fd7c7986d/src/share/classes/sun/misc/Unsafe.java
 *
 * The Segment is created eagerly as soon as the partition for the dictionary
 * is created, but the memory is allocated lazily.
 */
public class Segment {

    private final AtomicReference<SegmentTask> ref = new AtomicReference<>();
    private final SerializationService serializationService;
    private final DictionaryConfig config;
    private final EntryModel model;
    private final EntryEncoder encoder;


    private KeyTable keyTable;
    private DataRegion dataRegion;
    private boolean allocated;

    public Segment(SerializationService serializationService,
                   EntryModel model,
                   EntryEncoder encoder,
                   DictionaryConfig config) {
        this.serializationService = serializationService;
        this.config = config;
        this.model = model;
        this.encoder = encoder;
        this.dataRegion = new DataRegion(config, encoder, model);
        //todo: this part is ugly
        this.keyTable = new KeyTable(config.getInitialSegmentSize());
    }

    private void ensureAllocated() {
        if (!allocated) {
            allocated = true;
            dataRegion.alloc();
            keyTable.alloc();
        }
    }

    // todo: count could be volatile size it can be accessed by any thread.
    public int count() {
        return dataRegion.count();
    }

    public void put(Data keyData, int partitionHash, Data valueData) {
        ensureAllocated();

        // creating these objects can cause performance problems. E.g. when the value is a large
        // byte array. So we should not be forcing to pull these objects to memory.
        Object key = serializationService.toObject(keyData);
        Object value = serializationService.toObject(valueData);

        int offset = keyTable.search(key, partitionHash);

        if (offset == -1) {
            //System.out.println("previous value not found, inserting");
            offset = dataRegion.insert(key, value);
            keyTable.insert(key, partitionHash, offset);
        } else {
            //System.out.println("previous value found, overwriting");
            // todo: we assume that we can overwrite; but with variable length records this doesn't need to be the case
            dataRegion.overwrite(value, offset);
        }
    }

    public Object get(Data keyData, int partitionHash) {
        if (!allocated) {
            // no memory has been allocated, so no items are stored.
            return null;
        }

        Object key = serializationService.toObject(keyData);
        int offset = keyTable.search(key, partitionHash);
        return offset == -1 ? null : dataRegion.readValue(offset);
    }

    /**
     * Executes the task on this segment.
     *
     * The task is executed immediately if the segment is available, or parked for later
     * execution when the semgnet is in use.
     *
     * @param task
     * @return true if the task got executed, false if the task is appended for later execution.
     */
    public boolean execute(SegmentTask task) {
        return false;
    }

    public void clear() {
        dataRegion.clear();
        keyTable.clear();
    }
}
