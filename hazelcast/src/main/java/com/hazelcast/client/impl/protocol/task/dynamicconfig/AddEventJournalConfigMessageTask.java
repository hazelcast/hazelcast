/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.task.dynamicconfig;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddEventJournalConfigCodec;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.StringUtil;

public class AddEventJournalConfigMessageTask
        extends AbstractAddConfigMessageTask<DynamicConfigAddEventJournalConfigCodec.RequestParameters> {

    public AddEventJournalConfigMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected DynamicConfigAddEventJournalConfigCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return DynamicConfigAddEventJournalConfigCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return DynamicConfigAddEventJournalConfigCodec.encodeResponse();
    }

    @Override
    protected IdentifiedDataSerializable getConfig() {
        EventJournalConfig config = new EventJournalConfig();
        if (StringUtil.isNullOrEmpty(parameters.mapName) && StringUtil.isNullOrEmpty(parameters.cacheName)) {
            throw new IllegalArgumentException("Event journal config should have non-empty map name and/or cache name");
        }
        if (!StringUtil.isNullOrEmpty(parameters.mapName)) {
            config.setMapName(parameters.mapName);
        }
        if (!StringUtil.isNullOrEmpty(parameters.cacheName)) {
            config.setCacheName(parameters.cacheName);
        }
        config.setEnabled(parameters.enabled);
        config.setTimeToLiveSeconds(parameters.timeToLiveSeconds);
        config.setCapacity(parameters.capacity);
        return config;
    }

    @Override
    public String getMethodName() {
        return "addEventJournalConfig";
    }
}
