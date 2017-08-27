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
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.nio.Bits;

/**
 * Codec for {@link MapIndexConfig}
 */
@Codec(MapIndexConfig.class)
@Since("1.5")
public final class MapIndexConfigCodec {

    private MapIndexConfigCodec() {
    }

    public static MapIndexConfig decode(ClientMessage clientMessage) {
        MapIndexConfig config = new MapIndexConfig();
        config.setAttribute(clientMessage.getStringUtf8());
        config.setOrdered(clientMessage.getBoolean());
        return config;
    }

    public static void encode(MapIndexConfig config, ClientMessage clientMessage) {
        clientMessage.set(config.getAttribute()).set(config.isOrdered());
    }

    public static int calculateDataSize(MapIndexConfig config) {
        int dataSize = Bits.BOOLEAN_SIZE_IN_BYTES;
        dataSize += ParameterUtil.calculateDataSize(config.getAttribute());
        return dataSize;
    }
}
