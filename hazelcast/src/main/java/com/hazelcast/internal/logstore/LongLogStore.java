/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.logstore;

import com.hazelcast.log.encoders.LongEncoder;

import java.util.function.BinaryOperator;
import java.util.function.LongBinaryOperator;

import static com.hazelcast.internal.nio.Bits.LONG_SIZE_IN_BYTES;

public class LongLogStore extends LogStore<Long> {

    public LongLogStore(LogStoreConfig config) {
        super(config);
    }

    @Override
    public long putObject(Long o) {
        return putLong(o);
    }

    @Override
    public Long getObject(long sequence) {
        return getLong(sequence);
    }

    @Override
    public Long reduce(BinaryOperator<Long> accumulator) {
        throw new UnsupportedOperationException();
    }

    public long putLong(long v) {
        ensureCapacity(LONG_SIZE_IN_BYTES);
        updateEdenTime();

        UNSAFE.putLong(eden.address + eden.position, v);
        long sequence = eden.position + eden.startSequence;
        eden.position += LONG_SIZE_IN_BYTES;
        eden.count++;
        return sequence;
    }

    public long getLong(long sequence) {
        long address = toAddress(sequence);
        return UNSAFE.getLong(address);
    }

    public Object reduce(LongBinaryOperator accumulator) {
        if (!type.equals(Long.TYPE)) {
            throw new IllegalStateException();
        }

        long edenLimit = eden.position;
        long pos = 0;
        long consumed = 0;
        boolean isFirst = true;
        long prev = 0;
        LongEncoder encoder = (LongEncoder) this.encoder;
        if (edenLimit > 0) {
            while (pos <= eden.position + LONG_SIZE_IN_BYTES) {
                long address = eden.address + pos;
                long object = encoder.decodeLong(address);
                pos += LONG_SIZE_IN_BYTES;
                if (isFirst) {
                    isFirst = false;
                    prev = object;
                } else {
                    prev = accumulator.applyAsLong(prev, object);
                }
            }
        }

//        Segment segment = first;
//        while (segment != null) {
//            pos = 0;
//            do {
//                long address = segment.address + pos;
//                Object object = encoder.decode(address, consumed);
//                if (consumed.value == 0) {
//                    break;
//                }
//
//                if (prev == null) {
//                    prev = object;
//                } else {
//                    prev = accumulator.applyAsLong(prev, object);
//                }
//
//                pos += consumed.value;
//                consumed.value = 0;
//            } while (pos < config.segmentSize);
//
//            segment = segment.next;
//        }

        System.out.println("result:" + prev);
        return prev;
    }
}
