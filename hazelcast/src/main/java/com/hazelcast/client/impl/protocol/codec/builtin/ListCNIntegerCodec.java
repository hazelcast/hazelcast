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

package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.client.impl.protocol.ClientMessage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.INT_SIZE_IN_BYTES;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.decodeByte;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.decodeInteger;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.encodeByte;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.encodeInteger;

public final class ListCNIntegerCodec {

    private static final byte TYPE_NULL_ONLY = 1;
    private static final byte TYPE_NOT_NULL_ONLY = 2;
    private static final byte TYPE_MIXED = 3;

    private ListCNIntegerCodec() {
    }

    public static void encode(ClientMessage clientMessage, Collection<Integer> collection) {
        int totalItemCount = collection.size();
        int notNullItemCount = countNotNullItems(collection);

        int frameSize = getFrameSize(notNullItemCount, totalItemCount);

        ClientMessage.Frame frame = new ClientMessage.Frame(new byte[frameSize]);

        if (notNullItemCount == 0) {
            encodeHeader(frame, TYPE_NULL_ONLY, totalItemCount);
        } else if (notNullItemCount == totalItemCount) {
            encodeHeader(frame, TYPE_NOT_NULL_ONLY, totalItemCount);

            Iterator<Integer> iterator = collection.iterator();

            for (int i = 0; i < totalItemCount; i++) {
                encodeInteger(frame.content, 5 + i * INT_SIZE_IN_BYTES, iterator.next());
            }
        } else {
            encodeHeader(frame, TYPE_MIXED, totalItemCount);

            int bitmapPosition = 5;
            int nextItemPosition = bitmapPosition + 1;

            int bitmap = 0;
            int trackedItems = 0;

            for (Integer item : collection) {
                if (item != null) {
                    bitmap = bitmap | 1 << trackedItems;

                    encodeInteger(frame.content, nextItemPosition, item);
                }

                if (++trackedItems == 8) {
                    encodeByte(frame.content, bitmapPosition, (byte) bitmap);

                    bitmapPosition = nextItemPosition;
                    nextItemPosition = bitmapPosition + 1;
                    bitmap = 0;
                    trackedItems = 0;
                }
            }

            if (trackedItems != 0) {
                encodeByte(frame.content, bitmapPosition, (byte) bitmap);
            }
        }

        clientMessage.add(frame);
    }

    private static void encodeHeader(ClientMessage.Frame frame, byte type, int size) {
        encodeByte(frame.content, 0, type);
        encodeInteger(frame.content, 1, size);
    }

    public static List<Integer> decode(ClientMessage.ForwardFrameIterator iterator) {
        return decode(iterator.next());
    }

    public static List<Integer> decode(ClientMessage.Frame frame) {
        byte type = decodeByte(frame.content, 0);
        int count = decodeInteger(frame.content, 1);

        ArrayList<Integer> res = new ArrayList<>(count);

        switch (type) {
            case TYPE_NULL_ONLY:
                for (int i = 0; i < count; i++) {
                    res.add(null);
                }

                break;

            case TYPE_NOT_NULL_ONLY:
                for (int i = 0; i < count; i++) {
                    res.add(decodeInteger(frame.content, 5 + i * INT_SIZE_IN_BYTES));
                }

                break;

            default:
                assert type == TYPE_MIXED;

                int position = 5;

                int readCount = 0;

                while (readCount < count) {
                    int bitmap = decodeByte(frame.content, position++);

                    for (int i = 0; i < 8 && readCount < count; i++) {
                        int mask = 1 << i;

                        if ((bitmap & mask) == mask) {
                            res.add(decodeInteger(frame.content, position));

                            position += INT_SIZE_IN_BYTES;
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

    private static int countNotNullItems(Collection<Integer> collection) {
        int res = 0;

        for (Integer item : collection) {
            if (item != null) {
                res++;
            }
        }

        return res;
    }

    private static int getFrameSize(int nonNullItemCount, int totalItemCount) {
        int payload;

        if (nonNullItemCount == 0) {
            // Only nulls. Write only size.
            payload = 0;
        } else if (nonNullItemCount == totalItemCount) {
            // Only non-nulls. Write without a bitmap.
            payload = totalItemCount * INT_SIZE_IN_BYTES;
        } else {
            // Mixed null and non-nulls. Write with a bitmap.
            int nonNullItemCumulativeSize = nonNullItemCount * INT_SIZE_IN_BYTES;
            int bitmapCumulativeSize = totalItemCount / 8 + (totalItemCount % 8 > 0 ? 1 : 0);

            payload = nonNullItemCumulativeSize + bitmapCumulativeSize;
        }

        // Add the payload type byte.
        return 1 + INT_SIZE_IN_BYTES + payload;
    }
}
