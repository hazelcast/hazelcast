/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.networking.nio;

import com.hazelcast.instance.ProtocolType;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.management.JsonSerializable;
import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.spi.annotation.PrivateApi;

import java.util.EnumMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.util.JsonUtil.getObject;

/**
 * Stats per {@link ProtocolType} for a single direction of network traffic (inbound or outbound).
 * <p>
 * Stores number of bytes sent or received per {@link ProtocolType} depending on whether it is
 * for inbound or outbound traffic. Works only when Advanced Networking is enabled and
 * {@link com.hazelcast.config.EndpointConfig} is added for the {@link ProtocolType} in question.
 */
@PrivateApi
public final class AdvancedNetworkStats implements JsonSerializable {

    private final EnumMap<ProtocolType, AtomicLong> bytesTransceived;

    public AdvancedNetworkStats() {
        bytesTransceived = new EnumMap<ProtocolType, AtomicLong>(ProtocolType.class);
        for (ProtocolType type : ProtocolType.valuesAsSet()) {
            bytesTransceived.put(type, new AtomicLong());
        }
    }

    public void setBytesTransceivedForProtocol(ProtocolType protocolType, long bytes) {
        bytesTransceived.get(protocolType).lazySet(bytes);
    }

    // used for tests only
    public long getBytesTransceivedForProtocol(ProtocolType protocolType) {
        return bytesTransceived.get(protocolType).get();
    }

    /**
     * Dynamically registers probes for each protocol type. All registered probes will have the
     * same prefix plus the protocol type as their names.
     *
     * @param metricsRegistry {@link MetricsRegistry} instance to register the probes on
     * @param prefix          prefix for the probe names to be registered
     */
    public void registerMetrics(MetricsRegistry metricsRegistry, String prefix) {
        for (final ProtocolType type : ProtocolType.valuesAsSet()) {
            metricsRegistry.register(this, prefix + "." + type.name(), ProbeLevel.INFO,
                    new LongProbeFunction<AdvancedNetworkStats>() {
                        @Override
                        public long get(AdvancedNetworkStats source) {
                            return bytesTransceived.get(type).get();
                        }
                    });
        }
    }

    /**
     * For serializing the stats before sending to Management Center.
     *
     * @return the JSON representation of this object
     */
    @Override
    public JsonObject toJson() {
        JsonObject bytesTransceivedJson = new JsonObject();
        for (ProtocolType type : ProtocolType.valuesAsSet()) {
            bytesTransceivedJson.add(type.name(), bytesTransceived.get(type).get());
        }

        JsonObject result = new JsonObject();
        result.add("bytesTransceived", bytesTransceivedJson);
        return result;
    }

    /**
     * Extracts the state from the given {@code json} object and mutates the
     * state of this object.
     *
     * @param json the JSON object carrying state for this object
     */
    @Override
    public void fromJson(JsonObject json) {
        JsonObject bytesTransceivedJson = getObject(json, "bytesTransceived", null);
        if (bytesTransceivedJson != null) {
            for (ProtocolType type : ProtocolType.valuesAsSet()) {
                bytesTransceived.get(type).set(bytesTransceivedJson.getLong(type.name(), 0));
            }
        }
    }

}
