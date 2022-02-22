/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.task.replicatedmap;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ReplicatedMapAddEntryListenerToKeyWithPredicateCodec;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.query.Predicate;

import java.util.UUID;

public class ReplicatedMapAddEntryListenerToKeyWithPredicateMessageTask
        extends AbstractReplicatedMapAddEntryListenerMessageTask
        <ReplicatedMapAddEntryListenerToKeyWithPredicateCodec.RequestParameters> {

    public ReplicatedMapAddEntryListenerToKeyWithPredicateMessageTask(ClientMessage clientMessage, Node node,
                                                                      Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    public Predicate getPredicate() {
        return serializationService.toObject(parameters.predicate);
    }

    @Override
    public Data getKey() {
        return parameters.key;
    }

    @Override
    protected boolean isLocalOnly() {
        return parameters.localOnly;
    }

    @Override
    protected ClientMessage encodeEvent(Data key, Data newValue, Data oldValue, Data mergingValue,
                                        int type, UUID uuid, int numberOfAffectedEntries) {
        return ReplicatedMapAddEntryListenerToKeyWithPredicateCodec.encodeEntryEvent(key, newValue,
                oldValue, mergingValue, type, uuid, numberOfAffectedEntries);
    }

    @Override
    protected ReplicatedMapAddEntryListenerToKeyWithPredicateCodec.RequestParameters
    decodeClientMessage(ClientMessage clientMessage) {
        return ReplicatedMapAddEntryListenerToKeyWithPredicateCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ReplicatedMapAddEntryListenerToKeyWithPredicateCodec.encodeResponse((UUID) response);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{null, parameters.predicate, parameters.key};
    }
}
