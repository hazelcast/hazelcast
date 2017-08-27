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
import com.hazelcast.config.NearCachePreloaderConfig;
import com.hazelcast.nio.Bits;

@Codec(NearCachePreloaderConfig.class)
@Since("1.5")
public final class NearCachePreloaderConfigCodec {

    private NearCachePreloaderConfigCodec() {
    }

    public static NearCachePreloaderConfig decode(ClientMessage clientMessage) {
        NearCachePreloaderConfig config = new NearCachePreloaderConfig();
        config.setEnabled(clientMessage.getBoolean());
        config.setDirectory(clientMessage.getStringUtf8());
        config.setStoreInitialDelaySeconds(clientMessage.getInt());
        config.setStoreIntervalSeconds(clientMessage.getInt());
        return config;
    }

    public static void encode(NearCachePreloaderConfig config, ClientMessage clientMessage) {
        clientMessage.set(config.isEnabled())
                     .set(config.getDirectory())
                     .set(config.getStoreInitialDelaySeconds())
                     .set(config.getStoreIntervalSeconds());
    }

    public static int calculateDataSize(NearCachePreloaderConfig config) {
        int dataSize = Bits.BOOLEAN_SIZE_IN_BYTES + 2 * Bits.INT_SIZE_IN_BYTES;
        dataSize += ParameterUtil.calculateDataSize(config.getDirectory());
        return dataSize;
    }
}
