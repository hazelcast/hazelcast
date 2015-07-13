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

package com.hazelcast.transaction.client;

import com.hazelcast.client.impl.client.MultiTargetClientRequest;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.security.permission.TransactionPermission;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.impl.SerializableList;
import com.hazelcast.transaction.impl.xa.CollectRemoteTransactionsOperationFactory;
import com.hazelcast.transaction.impl.xa.XAService;

import java.security.Permission;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class CollectXATransactionsRequest extends MultiTargetClientRequest {

    public CollectXATransactionsRequest() {
    }

    @Override
    protected OperationFactory createOperationFactory() {
        return new CollectRemoteTransactionsOperationFactory();
    }

    @Override
    protected Object reduce(Map<Address, Object> map) {
        List<Data> set = new ArrayList<Data>();
        for (Object o : map.values()) {
            SerializableList xidSet = (SerializableList) o;
            set.addAll(xidSet.getCollection());
        }
        return new SerializableList(set);
    }

    @Override
    public Collection<Address> getTargets() {
        Collection<MemberImpl> memberList = getClientEngine().getClusterService().getMemberList();
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
    public int getFactoryId() {
        return ClientTxnPortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ClientTxnPortableHook.COLLECT_XA;
    }

    @Override
    public Permission getRequiredPermission() {
        return new TransactionPermission();
    }
}
