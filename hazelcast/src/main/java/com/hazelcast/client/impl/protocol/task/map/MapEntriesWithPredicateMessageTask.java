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
import com.hazelcast.client.impl.protocol.parameters.DataEntryListResultParameters;
import com.hazelcast.client.impl.protocol.parameters.MapEntriesWithPredicateParameters;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryResultEntry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class MapEntriesWithPredicateMessageTask extends AbstractMapQueryMessageTask<MapEntriesWithPredicateParameters> {

    public MapEntriesWithPredicateMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected ClientMessage reduce(Collection<QueryResultEntry> result) {
        List<Data> keys = new ArrayList<Data>();
        List<Data> values = new ArrayList<Data>();
        for (QueryResultEntry resultEntry : result) {
            keys.add(resultEntry.getKeyData());
            values.add(resultEntry.getValueData());
        }
        return DataEntryListResultParameters.encode(keys, values);
    }

    @Override
    protected Predicate getPredicate() {
        return serializationService.toObject(parameters.predicate);
    }

    @Override
    protected MapEntriesWithPredicateParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapEntriesWithPredicateParameters.decode(clientMessage);
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
        return "entries";
    }

}
