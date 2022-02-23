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

package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.internal.util.BiTuple;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.BYTE_SIZE_IN_BYTES;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.INT_SIZE_IN_BYTES;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.decodeByte;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.decodeInt;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.encodeByte;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.encodeInt;

/**
 * A common codec that is used to serialize a collections with a fixed-size nullable elements.
 */
final class ListCNFixedSizeCodec {

    private static final byte TYPE_NULL_ONLY = 1;
    private static final byte TYPE_NOT_NULL_ONLY = 2;
    private static final byte TYPE_MIXED = 3;

    private static final int ITEMS_PER_BITMASK = 8;

    private static final int HEADER_SIZE = BYTE_SIZE_IN_BYTES + INT_SIZE_IN_BYTES;

    private ListCNFixedSizeCodec() {
    }

    public static <T> void encode(
        ClientMessage clientMessage,
        Iterable<T> items,
        int itemSizeInBytes,
        EncodeFunction<T> encodeFunction
    ) {
        BiTuple<Integer, Integer> itemCounts = countItems(items);

        int totalItemCount = itemCounts.element1;
        int nonNullItemCount = itemCounts.element2;

        int frameSize = getFrameSize(nonNullItemCount, totalItemCount, itemSizeInBytes);

        ClientMessage.Frame frame = new ClientMessage.Frame(new byte[frameSize]);

        if (nonNullItemCount == 0) {
            encodeHeader(frame, TYPE_NULL_ONLY, totalItemCount);
        } else if (nonNullItemCount == totalItemCount) {
            encodeHeader(frame, TYPE_NOT_NULL_ONLY, totalItemCount);

            Iterator<T> iterator = items.iterator();

            for (int i = 0; i < totalItemCount; i++) {
                encodeFunction.encode(frame.content, HEADER_SIZE + i * itemSizeInBytes, iterator.next());
            }
        } else {
            encodeHeader(frame, TYPE_MIXED, totalItemCount);

            int bitmaskPosition = HEADER_SIZE;
            int nextItemPosition = bitmaskPosition + BYTE_SIZE_IN_BYTES;

            int bitmask = 0;
            int trackedItems = 0;

            for (T item : items) {
                if (item != null) {
                    bitmask = bitmask | 1 << trackedItems;

                    encodeFunction.encode(frame.content, nextItemPosition, item);

                    nextItemPosition += itemSizeInBytes;
                }

                if (++trackedItems == ITEMS_PER_BITMASK) {
                    encodeByte(frame.content, bitmaskPosition, (byte) bitmask);

                    bitmaskPosition = nextItemPosition;
                    nextItemPosition = bitmaskPosition + BYTE_SIZE_IN_BYTES;
                    bitmask = 0;
                    trackedItems = 0;
                }
            }

            if (trackedItems != 0) {
                encodeByte(frame.content, bitmaskPosition, (byte) bitmask);
            }
        }

        clientMessage.add(frame);
    }

    private static void encodeHeader(ClientMessage.Frame frame, byte type, int size) {
        encodeByte(frame.content, 0, type);
        encodeInt(frame.content, 1, size);
    }

    public static <T> List<T> decode(
        ClientMessage.Frame frame,
        int itemSizeInBytes,
        DecodeFunction<T> decodeFunction
    ) {
        byte type = decodeByte(frame.content, 0);
        int count = decodeInt(frame.content, 1);

        ArrayList<T> res = new ArrayList<>(count);

        switch (type) {
            case TYPE_NULL_ONLY:
                for (int i = 0; i < count; i++) {
                    res.add(null);
                }

                break;

            case TYPE_NOT_NULL_ONLY:
                for (int i = 0; i < count; i++) {
                    res.add(decodeFunction.decode(frame.content, HEADER_SIZE + i * itemSizeInBytes));
                }

                break;

            default:
                assert type == TYPE_MIXED;

                int position = HEADER_SIZE;

                int readCount = 0;

                while (readCount < count) {
                    int bitmask = decodeByte(frame.content, position++);

                    for (int i = 0; i < ITEMS_PER_BITMASK && readCount < count; i++) {
                        int mask = 1 << i;

                        if ((bitmask & mask) == mask) {
                            res.add(decodeFunction.decode(frame.content, position));

                            position += itemSizeInBytes;
                        } else {
                            res.add(null);
                        }

                        readCount++;
                    }
                }

                assert readCount == res.size();
        }

        return res;
    }

    private static <T> BiTuple<Integer, Integer> countItems(Iterable<T> items) {
        int total = 0;
        int nonNull = 0;

        for (T item : items) {
            total++;

            if (item != null) {
                nonNull++;
            }
        }

        return BiTuple.of(total, nonNull);
    }

    private static int getFrameSize(int nonNullItemCount, int totalItemCount, int itemSizeInBytes) {
        int payload;

        if (nonNullItemCount == 0) {
            // Only nulls. Write only size.
            payload = 0;
        } else if (nonNullItemCount == totalItemCount) {
            // Only non-nulls. Write without a bitmask.
            payload = totalItemCount * itemSizeInBytes;
        } else {
            // Mixed null and non-nulls. Write with a bitmask.
            int nonNullItemCumulativeSize = nonNullItemCount * itemSizeInBytes;
            int bitmaskCumulativeSize = totalItemCount / ITEMS_PER_BITMASK + (totalItemCount % ITEMS_PER_BITMASK > 0 ? 1 : 0);

            payload = nonNullItemCumulativeSize + bitmaskCumulativeSize;
        }

        return HEADER_SIZE + payload;
    }

    @FunctionalInterface
    public interface EncodeFunction<T> {
        void encode(byte[] buffer, int position, T item);
    }

    @FunctionalInterface
    public interface DecodeFunction<T> {
        T decode(byte[] buffer, int position);
    }
}
