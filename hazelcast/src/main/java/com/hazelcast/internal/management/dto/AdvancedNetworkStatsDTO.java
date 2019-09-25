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

package com.hazelcast.internal.management.dto;

import com.hazelcast.instance.ProtocolType;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.management.JsonSerializable;
import com.hazelcast.internal.networking.nio.AdvancedNetworkStats;

import static com.hazelcast.internal.util.JsonUtil.getObject;

/**
 * A JSON representation of {@link AdvancedNetworkStats}.
 */
public class AdvancedNetworkStatsDTO implements JsonSerializable {

    private AdvancedNetworkStats advancedNetworkStats;

    public AdvancedNetworkStatsDTO() {
    }

    public AdvancedNetworkStatsDTO(AdvancedNetworkStats advancedNetworkStats) {
        this.advancedNetworkStats = advancedNetworkStats;
    }

    public AdvancedNetworkStats getAdvancedNetworkStats() {
        return advancedNetworkStats;
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
            bytesTransceivedJson.add(
                    type.name(),
                    advancedNetworkStats != null ? advancedNetworkStats.getBytesTransceivedForProtocol(type) : 0
            );
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
        advancedNetworkStats = new AdvancedNetworkStats();
        JsonObject bytesTransceivedJson = getObject(json, "bytesTransceived", null);
        if (bytesTransceivedJson != null) {
            for (ProtocolType type : ProtocolType.valuesAsSet()) {
                advancedNetworkStats.setBytesTransceivedForProtocol(type, bytesTransceivedJson.getLong(type.name(), 0));
            }
        }
    }

    @Override
    public String toString() {
        return "AdvancedNetworkStatsDTO{"
                + "advancedNetworkStats=" + advancedNetworkStats
                + '}';
    }

}
