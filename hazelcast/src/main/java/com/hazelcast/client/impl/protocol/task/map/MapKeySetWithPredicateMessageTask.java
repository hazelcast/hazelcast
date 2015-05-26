/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.task.map;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MapKeySetWithPredicateCodec;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryResultEntry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class MapKeySetWithPredicateMessageTask
        extends AbstractMapQueryMessageTask<MapKeySetWithPredicateCodec.RequestParameters> {

    public MapKeySetWithPredicateMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object reduce(Collection<QueryResultEntry> result) {
        List<Data> keys = new ArrayList<Data>(result.size());
        for (QueryResultEntry resultEntry : result) {
            keys.add(resultEntry.getKeyData());
        }
        return keys;
    }

    @Override
    protected Predicate getPredicate() {
        return serializationService.toObject(parameters.predicate);
    }

    @Override
    protected MapKeySetWithPredicateCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapKeySetWithPredicateCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MapKeySetWithPredicateCodec.encodeResponse((Collection<Data>) response);
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.predicate};
    }


    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return "keySet";
    }
}
