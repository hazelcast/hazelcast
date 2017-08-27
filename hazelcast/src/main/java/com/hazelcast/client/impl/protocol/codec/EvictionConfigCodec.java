/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.codec;

import com.hazelcast.annotation.Codec;
import com.hazelcast.annotation.Since;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.EvictionConfigHolder;
import com.hazelcast.client.impl.protocol.util.ParameterUtil;
import com.hazelcast.nio.serialization.Data;

import static com.hazelcast.nio.Bits.BOOLEAN_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;

@Codec(EvictionConfigHolder.class)
@Since("1.5")
public final class EvictionConfigCodec {

    private EvictionConfigCodec() {

    }

    public static EvictionConfigHolder decode(ClientMessage clientMessage) {
        int size = clientMessage.getInt();
        String maxSizePolicy = clientMessage.getStringUtf8();
        String evictionPolicy = clientMessage.getStringUtf8();
        boolean isNullComparatorClassName = clientMessage.getBoolean();
        String comparatorClassName = null;
        if (!isNullComparatorClassName) {
            comparatorClassName = clientMessage.getStringUtf8();
        }
        boolean isNullComparator = clientMessage.getBoolean();
        Data comparator = null;
        if (!isNullComparator) {
            comparator = clientMessage.getData();
        }
        return new EvictionConfigHolder(size, maxSizePolicy, evictionPolicy, comparatorClassName, comparator);
    }

    public static void encode(EvictionConfigHolder holder, ClientMessage clientMessage) {
        clientMessage.set(holder.getSize()).set(holder.getMaxSizePolicy())
                     .set(holder.getEvictionPolicy());
        boolean isNullComparatorClassName = holder.getComparatorClassName() == null;
        clientMessage.set(isNullComparatorClassName);
        if (!isNullComparatorClassName) {
            clientMessage.set(holder.getComparatorClassName());
        }
        boolean isNullComparator = holder.getComparator() == null;
        clientMessage.set(isNullComparator);
        if (!isNullComparator) {
            clientMessage.set(holder.getComparator());
        }
    }

    public static int calculateDataSize(EvictionConfigHolder holder) {
        int dataSize = 2 * BOOLEAN_SIZE_IN_BYTES + INT_SIZE_IN_BYTES;
        dataSize += ParameterUtil.calculateDataSize(holder.getMaxSizePolicy());
        dataSize += ParameterUtil.calculateDataSize(holder.getEvictionPolicy());
        if (holder.getComparator() != null) {
            dataSize += ParameterUtil.calculateDataSize(holder.getComparator());
        }
        if (holder.getComparatorClassName() != null) {
            dataSize += ParameterUtil.calculateDataSize(holder.getComparatorClassName());
        }
        return dataSize;
    }
}
