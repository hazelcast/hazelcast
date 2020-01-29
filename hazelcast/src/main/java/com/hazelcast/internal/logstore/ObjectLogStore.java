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

import com.hazelcast.log.encoders.Consumed;

import java.util.function.BinaryOperator;

public class ObjectLogStore<E> extends LogStore<E> {

    public ObjectLogStore(LogStoreConfig config) {
        super(config);
    }

    @Override
    public long putObject(E o) {
        ensureCapacity(1);
        updateEdenTime();

        long address = eden.address + eden.position;
        int remaining = segmentSize - eden.position;
        int written = encoder.encode(address, remaining, o);
        if (written < 0) {
            int required = -written;
            if (required > segmentSize) {
                throw new IllegalStateException("Can't write object:" + o + ", object can't span multiple segments "
                        + "it encodes to " + required
                        + "" + "but the maximum segment size is: " + segmentSize);
            }
            ensureCapacity(required);

            address = eden.address;
            remaining = segmentSize - eden.position;
            written = encoder.encode(address, remaining, o);
            if (written < 0) {
                throw new IllegalStateException("written:" + written + " remaining:" + remaining);
            }
        }

        long sequence = eden.position + eden.startSequence;
        eden.position += written;
        eden.count++;
        return sequence;
    }

    @Override
    public E getObject(long sequence) {
        long address = toAddress(sequence);

        // todo: ugly litter
        Consumed consumed = new Consumed();
        return (E) encoder.decode(address, consumed);
    }

    public E reduce(BinaryOperator<E> accumulator) {
        long edenLimit = eden.position;
        long pos = 0;
        Consumed consumed = new Consumed();
        E prev = null;
        if (edenLimit > 0) {
            do {
                long address = eden.address + pos;
                E object = encoder.decode(address, consumed);
                if (prev == null) {
                    prev = object;
                } else {
                    prev = accumulator.apply(prev, object);
                }
                pos += consumed.value;
                consumed.value = 0;
            } while (pos < segmentSize);
        }

        Segment segment = first;
        while (segment != null) {
            pos = 0;
            do {
                long address = segment.address + pos;
                E object = encoder.decode(address, consumed);
                if (consumed.value == 0) {
                    break;
                }

                if (prev == null) {
                    prev = object;
                } else {
                    prev = accumulator.apply(prev, object);
                }

                pos += consumed.value;
                consumed.value = 0;
            } while (pos < segmentSize);

            segment = segment.next;
        }

        System.out.println("result:" + prev);
        return prev;
    }
}
