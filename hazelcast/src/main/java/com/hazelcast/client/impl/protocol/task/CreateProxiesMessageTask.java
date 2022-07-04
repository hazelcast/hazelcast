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

package com.hazelcast.client.impl.protocol.task;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientCreateProxiesCodec;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.proxyservice.impl.ProxyInfo;
import com.hazelcast.spi.impl.proxyservice.impl.operations.PostJoinProxyOperation;

import java.security.Permission;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;


public class CreateProxiesMessageTask extends AbstractMultiTargetMessageTask<List<Map.Entry<String, String>>>
        implements Supplier<Operation> {

    public CreateProxiesMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    protected Supplier<Operation> createOperationSupplier() {
        return this;
    }

    @Override
    public Operation get() {
        List<ProxyInfo> proxyInfos = new ArrayList<ProxyInfo>(parameters.size());
        for (Map.Entry<String, String> proxy : parameters) {
            proxyInfos.add(new ProxyInfo(proxy.getValue(), proxy.getKey(), endpoint.getUuid()));
        }
        return new PostJoinProxyOperation(proxyInfos);
    }

    @Override
    protected Object reduce(Map<Member, Object> map) throws Throwable {
        for (Object result : map.values()) {
            if (result instanceof Throwable && !(result instanceof MemberLeftException)) {
                throw (Throwable) result;
            }
        }
        return null;
    }

    @Override
    public Collection<Member> getTargets() {
        return nodeEngine.getClusterService().getMembers();
    }

    @Override
    protected List<Map.Entry<String, String>> decodeClientMessage(ClientMessage clientMessage) {
        return ClientCreateProxiesCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ClientCreateProxiesCodec.encodeResponse();
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getServiceName() {
        return null;
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
}
