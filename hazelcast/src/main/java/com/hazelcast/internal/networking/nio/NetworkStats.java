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
import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.ProbeLevel;

import java.util.EnumMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Stats per {@link ProtocolType} for a single direction of network traffic (inbound or outbound).
 * <p>
 * Stores number of bytes sent or received per {@link ProtocolType}
 * depending on whether it is for inbound or outbound traffic. Works
 * only when Advanced Networking is enabled and {@link com.hazelcast.config.EndpointConfig}
 * is added for the {@link ProtocolType} in question.
 **/
public class NetworkStats {
    private final EnumMap<ProtocolType, AtomicLong> bytesTransceived;

    NetworkStats() {
        bytesTransceived = new EnumMap<ProtocolType, AtomicLong>(ProtocolType.class);
        for (ProtocolType protocolType : ProtocolType.values()) {
            bytesTransceived.put(protocolType, new AtomicLong());
        }
    }

    void setBytesTransceivedForProtocol(ProtocolType protocolType, long bytes) {
        AtomicLong value = bytesTransceived.get(protocolType);
        value.lazySet(bytes);
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
    void registerMetrics(MetricsRegistry metricsRegistry, String prefix) {
        for (final ProtocolType protocolType : ProtocolType.values()) {
            metricsRegistry.register(this, prefix + "." + protocolType.name(), ProbeLevel.INFO,
                    new LongProbeFunction<NetworkStats>() {
                        @Override
                        public long get(NetworkStats source) {
                            return bytesTransceived.get(protocolType).get();
                        }
                    });
        }
    }

    /**
     * For serializing the stats before sending to Management Center.
     *
     * @return the JSON representation of this object
     */
    public JsonObject toJson() {
        JsonObject bytesTransceivedJson = new JsonObject();
        for (ProtocolType protocolType : ProtocolType.values()) {
            bytesTransceivedJson.add(protocolType.name(), bytesTransceived.get(protocolType).get());
        }

        JsonObject result = new JsonObject();
        result.add("bytesTransceived", bytesTransceivedJson);
        return result;
    }
}
