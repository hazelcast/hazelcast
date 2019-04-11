/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.datastream.impl.encoders;

import com.hazelcast.datastream.impl.RecordModel;
import com.hazelcast.internal.memory.impl.UnsafeUtil;
import sun.misc.Unsafe;

/**
 * Responsible for reading//writing a record from/to offheap.
 *
 * The concrete implementations will be generated at runtime.
 * <p>
 * todo: for every field there will probably be a method generated? e.g. putPrice and getPrice
 */
public abstract class RecordEncoder<R> {

    protected final Unsafe unsafe = UnsafeUtil.UNSAFE;
    protected final RecordModel recordModel;
    protected final long recordDataOffset;
    protected final int recordPayloadSize;

    protected RecordEncoder(RecordModel recordModel) {
        this.recordModel = recordModel;
        this.recordDataOffset = recordModel.getDataOffset();
        this.recordPayloadSize = recordModel.getPayloadSize();
    }

    public abstract R newInstance();

    public abstract void writeRecord(R record, long segmentAddress, int recordOffset, long indicesAddress);

    public abstract void readRecord(R record, long segmentAddress, int recordOffset);
}
