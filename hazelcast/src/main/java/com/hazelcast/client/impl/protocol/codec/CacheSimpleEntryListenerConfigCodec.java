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
import com.hazelcast.config.CacheSimpleEntryListenerConfig;
import com.hazelcast.nio.Bits;

@Codec(CacheSimpleEntryListenerConfig.class)
@Since("1.5")
public final class CacheSimpleEntryListenerConfigCodec {

    private static final int ENCODED_BOOLEANS = 4;

    private CacheSimpleEntryListenerConfigCodec() {
    }

    public static CacheSimpleEntryListenerConfig decode(ClientMessage clientMessage) {
        CacheSimpleEntryListenerConfig config = new CacheSimpleEntryListenerConfig();
        config.setSynchronous(clientMessage.getBoolean());
        config.setOldValueRequired(clientMessage.getBoolean());
        boolean entryListenerFactory_isNull = clientMessage.getBoolean();
        if (!entryListenerFactory_isNull) {
            config.setCacheEntryListenerFactory(clientMessage.getStringUtf8());
        }
        boolean entryEventFilterFactory_isNull = clientMessage.getBoolean();
        if (!entryEventFilterFactory_isNull) {
            config.setCacheEntryEventFilterFactory(clientMessage.getStringUtf8());
        }
        return config;
    }

    public static void encode(CacheSimpleEntryListenerConfig config, ClientMessage clientMessage) {
        clientMessage.set(config.isSynchronous()).set(config.isOldValueRequired());
        boolean entryListenerFactory_isNull = config.getCacheEntryListenerFactory() == null;
        clientMessage.set(entryListenerFactory_isNull);
        if (!entryListenerFactory_isNull) {
            clientMessage.set(config.getCacheEntryListenerFactory());
        }
        boolean entryEventFilterFactory_isNull = config.getCacheEntryEventFilterFactory() == null;
        clientMessage.set(entryEventFilterFactory_isNull);
        if (!entryEventFilterFactory_isNull) {
            clientMessage.set(config.getCacheEntryEventFilterFactory());
        }
    }

    public static int calculateDataSize(CacheSimpleEntryListenerConfig config) {
        int dataSize = ENCODED_BOOLEANS * Bits.BOOLEAN_SIZE_IN_BYTES;
        if (config.getCacheEntryListenerFactory() != null) {
            dataSize += ParameterUtil.calculateDataSize(config.getCacheEntryListenerFactory());
        }
        if (config.getCacheEntryEventFilterFactory() != null) {
            dataSize += ParameterUtil.calculateDataSize(config.getCacheEntryEventFilterFactory());
        }
        return dataSize;
    }
}
