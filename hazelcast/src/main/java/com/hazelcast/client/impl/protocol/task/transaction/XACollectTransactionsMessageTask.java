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

package com.hazelcast.client.impl.protocol.task.transaction;

import com.hazelcast.client.impl.CollectRemoteTransactionsOperationSupplier;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.XATransactionCollectTransactionsCodec;
import com.hazelcast.client.impl.protocol.task.AbstractMultiTargetMessageTask;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.security.permission.TransactionPermission;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.SerializableList;
import com.hazelcast.transaction.impl.xa.XAService;
import com.hazelcast.util.function.Supplier;

import java.security.Permission;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class XACollectTransactionsMessageTask
        extends AbstractMultiTargetMessageTask<XATransactionCollectTransactionsCodec.RequestParameters> {

    public XACollectTransactionsMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected XATransactionCollectTransactionsCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return XATransactionCollectTransactionsCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return XATransactionCollectTransactionsCodec.encodeResponse((List<Data>) response);
    }

    @Override
    protected Supplier<Operation> createOperationSupplier() {
        return new CollectRemoteTransactionsOperationSupplier();
    }

    @Override
    protected Object reduce(Map<Member, Object> map) throws Throwable {
        List<Data> list = new ArrayList<Data>();
        for (Object o : map.values()) {
            if (o instanceof Throwable) {
                if (o instanceof MemberLeftException) {
                    continue;
                }
                throw (Throwable) o;
            }
            SerializableList xidSet = (SerializableList) o;
            list.addAll(xidSet.getCollection());
        }
        return list;
    }

    @Override
    public Collection<Member> getTargets() {
        return clientEngine.getClusterService().getMembers();
    }

    @Override
    public String getServiceName() {
        return XAService.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return null;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }

    @Override
    public Permission getRequiredPermission() {
        return new TransactionPermission();
    }
}
