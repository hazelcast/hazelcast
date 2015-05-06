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

package com.hazelcast.client.impl.protocol.task.transaction;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.DataCollectionResultParameters;
import com.hazelcast.client.impl.protocol.parameters.XACollectTransactionsParameters;
import com.hazelcast.client.impl.protocol.task.AbstractMultiTargetMessageTask;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.security.permission.TransactionPermission;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.impl.SerializableCollection;
import com.hazelcast.transaction.impl.xa.CollectRemoteTransactionsOperationFactory;
import com.hazelcast.transaction.impl.xa.XAService;

import java.security.Permission;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

public class XACollectTransactionsMessageTask extends AbstractMultiTargetMessageTask<XACollectTransactionsParameters> {

    public XACollectTransactionsMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected XACollectTransactionsParameters decodeClientMessage(ClientMessage clientMessage) {
        return XACollectTransactionsParameters.decode(clientMessage);
    }

    @Override
    protected OperationFactory createOperationFactory() {
        return new CollectRemoteTransactionsOperationFactory();
    }

    @Override
    protected ClientMessage reduce(Map<Address, Object> map) throws Throwable {
        HashSet<Data> set = new HashSet<Data>();
        for (Object o : map.values()) {
            SerializableCollection xidSet = (SerializableCollection) o;
            set.addAll(xidSet.getCollection());
        }
        return DataCollectionResultParameters.encode(set);
    }

    @Override
    public Collection<Address> getTargets() {
        Collection<MemberImpl> memberList = clientEngine.getClusterService().getMemberList();
        Collection<Address> addresses = new HashSet<Address>();
        for (MemberImpl member : memberList) {
            addresses.add(member.getAddress());
        }
        return addresses;
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
