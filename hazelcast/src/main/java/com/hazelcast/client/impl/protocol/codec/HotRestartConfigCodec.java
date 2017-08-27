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
import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.nio.Bits;

@Codec(HotRestartConfig.class)
@Since("1.5")
public final class HotRestartConfigCodec {

    private HotRestartConfigCodec() {
    }

    public static HotRestartConfig decode(ClientMessage clientMessage) {
        boolean enabled = clientMessage.getBoolean();
        boolean fsync = clientMessage.getBoolean();
        HotRestartConfig config = new HotRestartConfig();
        config.setEnabled(enabled);
        config.setFsync(fsync);
        return config;
    }

    public static void encode(HotRestartConfig config, ClientMessage clientMessage) {
        clientMessage.set(config.isEnabled()).set(config.isFsync());
    }

    public static int calculateDataSize(HotRestartConfig config) {
        return 2 * Bits.BOOLEAN_SIZE_IN_BYTES;
    }
}
