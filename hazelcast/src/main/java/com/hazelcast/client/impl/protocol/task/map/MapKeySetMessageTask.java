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

package com.hazelcast.client.impl.protocol.task.map;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MapKeySetCodec;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.query.QueryResultRow;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.internal.util.IterationType;

import java.security.Permission;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.map.impl.LocalMapStatsUtil.incrementOtherOperationsCount;

public class MapKeySetMessageTask
        extends DefaultMapQueryMessageTask<String> {

    public MapKeySetMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object reduce(Collection<QueryResultRow> result) {
        List<Data> keys = new ArrayList<Data>(result.size());
        for (QueryResultRow resultEntry : result) {
            keys.add(resultEntry.getKey());
        }
        incrementOtherOperationsCount((MapService) getService(MapService.SERVICE_NAME), parameters);
        return keys;
    }

    @Override
    protected Predicate getPredicate() {
        return Predicates.alwaysTrue();
    }

    @Override
    protected IterationType getIterationType() {
        return IterationType.KEY;
    }

    @Override
    protected String decodeClientMessage(ClientMessage clientMessage) {
        return MapKeySetCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MapKeySetCodec.encodeResponse((List<Data>) response);
    }

    public Permission getRequiredPermission() {
        return new MapPermission(parameters, ActionConstants.ACTION_READ);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters;
    }

    @Override
    public String getMethodName() {
        return "keySet";
    }

    @Override
    public Object[] getParameters() {
        return null;
    }
}
