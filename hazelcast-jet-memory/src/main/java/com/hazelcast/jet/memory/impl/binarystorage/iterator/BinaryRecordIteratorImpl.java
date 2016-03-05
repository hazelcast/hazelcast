/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.memory.impl.binarystorage.iterator;

import com.hazelcast.jet.memory.impl.util.MemoryUtil;
import com.hazelcast.jet.memory.api.binarystorage.oalayout.OALayout;
import com.hazelcast.jet.memory.api.binarystorage.iterator.BinaryRecordIterator;

import java.util.NoSuchElementException;

public class BinaryRecordIteratorImpl implements BinaryRecordIterator {
    private long slotAddress;

    private long recordAddress;

    private boolean nextChecked;

    private final OALayout layout;

    private boolean lastNextResult;

    public BinaryRecordIteratorImpl(OALayout layout) {
        this.layout = layout;
    }

    @Override
    public boolean hasNext() {
        if (nextChecked) {
            return lastNextResult;
        }

        try {
            if (recordAddress == MemoryUtil.NULL_VALUE) {
                recordAddress = layout.getHeaderRecordAddress(slotAddress);
            } else {
                recordAddress = layout.getNextRecordAddress(recordAddress);
            }

            lastNextResult = recordAddress != MemoryUtil.NULL_VALUE;
            return lastNextResult;
        } finally {
            nextChecked = true;
        }
    }

    @Override
    public long next() {
        try {
            if (hasNext()) {
                return recordAddress;
            } else {
                throw new NoSuchElementException("iterator finished");
            }
        } finally {
            nextChecked = false;
        }
    }

    @Override
    public void reset(long slotAddress,
                      int sourceId) {
        this.nextChecked = false;
        this.lastNextResult = false;
        this.slotAddress = slotAddress;
        this.recordAddress = MemoryUtil.NULL_VALUE;
    }
}
