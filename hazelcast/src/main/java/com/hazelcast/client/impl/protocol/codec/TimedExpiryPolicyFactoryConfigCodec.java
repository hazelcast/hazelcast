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
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.DurationConfig;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig;
import com.hazelcast.nio.Bits;

import java.util.concurrent.TimeUnit;

@Codec(TimedExpiryPolicyFactoryConfig.class)
@Since("1.5")
public final class TimedExpiryPolicyFactoryConfigCodec {

    private TimedExpiryPolicyFactoryConfigCodec() {
    }

    public static TimedExpiryPolicyFactoryConfig decode(ClientMessage clientMessage) {
        String expiryPolicyType = clientMessage.getStringUtf8();
        long duration = clientMessage.getLong();
        String timeUnit = clientMessage.getStringUtf8();
        DurationConfig durationConfig = new DurationConfig(duration, TimeUnit.valueOf(timeUnit));
        TimedExpiryPolicyFactoryConfig config = new TimedExpiryPolicyFactoryConfig(
                TimedExpiryPolicyFactoryConfig.ExpiryPolicyType.valueOf(expiryPolicyType),
                durationConfig);
        return config;
    }

    public static void encode(TimedExpiryPolicyFactoryConfig config, ClientMessage clientMessage) {
        clientMessage.set(config.getExpiryPolicyType().name())
                     .set(config.getDurationConfig().getDurationAmount())
                     .set(config.getDurationConfig().getTimeUnit().name());
    }

    public static int calculateDataSize(TimedExpiryPolicyFactoryConfig config) {
        int dataSize = Bits.LONG_SIZE_IN_BYTES;
        dataSize += ParameterUtil.calculateDataSize(config.getExpiryPolicyType().name());
        dataSize += ParameterUtil.calculateDataSize(config.getDurationConfig().getTimeUnit().name());
        return dataSize;
    }
}
