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

package com.hazelcast.client.impl.protocol.task.dynamicconfig;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddMerkleTreeConfigCodec;
import com.hazelcast.config.MerkleTreeConfig;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.Preconditions;

/**
 * Member side task for adding a merkle tree configuration.
 */
public class AddMerkleTreeConfigMessageTask
        extends AbstractAddConfigMessageTask<DynamicConfigAddMerkleTreeConfigCodec.RequestParameters> {

    public AddMerkleTreeConfigMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected DynamicConfigAddMerkleTreeConfigCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return DynamicConfigAddMerkleTreeConfigCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return DynamicConfigAddMerkleTreeConfigCodec.encodeResponse();
    }

    @Override
    protected IdentifiedDataSerializable getConfig() {
        Preconditions.checkHasText(parameters.mapName, "Merkle tree config must define a map name");
        return new MerkleTreeConfig()
                .setMapName(parameters.mapName)
                .setEnabled(parameters.enabled)
                .setDepth(parameters.depth);
    }

    @Override
    public String getMethodName() {
        return "addMerkleTreeConfig";
    }
}
