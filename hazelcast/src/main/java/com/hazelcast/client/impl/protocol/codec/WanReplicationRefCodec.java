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
import com.hazelcast.client.impl.protocol.util.ParameterUtil;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.nio.Bits;

import java.util.ArrayList;
import java.util.List;

@Codec(WanReplicationRef.class)
@Since("1.5")
public final class WanReplicationRefCodec {

    private WanReplicationRefCodec() {
    }

    public static WanReplicationRef decode(ClientMessage clientMessage) {
        String name = clientMessage.getStringUtf8();
        String mergePolicy = clientMessage.getStringUtf8();
        boolean republishingEnabled = clientMessage.getBoolean();
        boolean isNullFilters = clientMessage.getBoolean();
        List<String> filters = null;
        if (!isNullFilters) {
            int filtersCount = clientMessage.getInt();
            filters = new ArrayList<String>(filtersCount);
            for (int i = 0; i < filtersCount; i++) {
                filters.add(clientMessage.getStringUtf8());
            }
        }
        return new WanReplicationRef(name, mergePolicy, filters, republishingEnabled);
    }

    public static void encode(WanReplicationRef ref, ClientMessage clientMessage) {
        boolean isNullFilters = ref.getFilters() == null;
        clientMessage.set(ref.getName()).set(ref.getMergePolicy()).set(ref.isRepublishingEnabled())
                     .set(isNullFilters);
        if (!isNullFilters) {
            clientMessage.set(ref.getFilters().size());
            for (String filter : ref.getFilters()) {
                clientMessage.set(filter);
            }
        }
    }

    public static int calculateDataSize(WanReplicationRef ref) {
        int dataSize = 2 * Bits.BOOLEAN_SIZE_IN_BYTES;
        dataSize += ParameterUtil.calculateDataSize(ref.getName());
        dataSize += ParameterUtil.calculateDataSize(ref.getMergePolicy());
        boolean hasFilters = ref.getFilters() != null && !ref.getFilters().isEmpty();
        if (hasFilters) {
            dataSize += Bits.INT_SIZE_IN_BYTES;
            for (String filter : ref.getFilters()) {
                dataSize += ParameterUtil.calculateDataSize(filter);
            }
        }
        return dataSize;
    }

}
