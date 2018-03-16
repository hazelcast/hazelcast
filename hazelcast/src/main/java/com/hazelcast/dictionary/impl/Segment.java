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

    private OffsetRegion offsetRegion;
    private DataRegion dataRegion;
    private volatile boolean allocated;


    public Segment(SerializationService serializationService,
                   EntryModel model,
                   EntryEncoder encoder,
                   DictionaryConfig config) {
        this.serializationService = serializationService;
        this.dataRegion = new DataRegion(config, encoder, model);
        //todo: this part is ugly
        this.offsetRegion = new OffsetRegion(config.getInitialSegmentSize());
    }

    public long allocated(){
        return offsetRegion.allocated()+dataRegion.allocated();
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

        int offset = offsetRegion.search(key, partitionHash);

        if (offset == -1) {
            //System.out.println("previous value not found, inserting");
            offset = dataRegion.insert(key, value);
            offsetRegion.insert(key, partitionHash, offset);
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
        int offset = offsetRegion.search(key, partitionHash);
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
        offsetRegion.clear();
    }

    public long consumed() {
        return dataRegion.consumed()+offsetRegion.consumed();
    }
}
