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

package com.hazelcast.spi.impl.proxyservice.impl.operations;

import com.hazelcast.cache.CacheNotExistsException;
import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.replicatedmap.ReplicatedMapCantBeCreatedOnLiteMemberException;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.SpiDataSerializerHook;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.proxyservice.impl.ProxyInfo;
import com.hazelcast.spi.impl.proxyservice.impl.ProxyRegistry;
import com.hazelcast.spi.impl.proxyservice.impl.ProxyServiceImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import static com.hazelcast.internal.util.EmptyStatement.ignore;

public class PostJoinProxyOperation extends Operation implements IdentifiedDataSerializable {

    private Collection<ProxyInfo> proxies;

    public PostJoinProxyOperation() {
    }

    public PostJoinProxyOperation(Collection<ProxyInfo> proxies) {
        this.proxies = proxies;
    }

    @Override
    public void run() throws Exception {
        if (proxies == null || proxies.size() <= 0) {
            return;
        }

        NodeEngine nodeEngine = getNodeEngine();
        ProxyServiceImpl proxyService = getService();
        ExecutionService executionService = nodeEngine.getExecutionService();
        for (final ProxyInfo proxy : proxies) {
            final ProxyRegistry registry = proxyService.getOrCreateRegistry(proxy.getServiceName());

            try {
                executionService.execute(ExecutionService.SYSTEM_EXECUTOR, new CreateProxyTask(registry, proxy));
            } catch (Throwable t) {
                logProxyCreationFailure(proxy, t);
            }
        }
    }

    private void logProxyCreationFailure(ProxyInfo proxy, Throwable t) {
        getLogger().severe("Cannot create proxy [" + proxy.getServiceName() + ":" + proxy.getObjectName() + "]!", t);
    }

    @Override
    public String getServiceName() {
        return ProxyServiceImpl.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        int len = proxies != null ? proxies.size() : 0;
        out.writeInt(len);
        if (len > 0) {
            for (ProxyInfo proxy : proxies) {
                out.writeString(proxy.getServiceName());
                out.writeString(proxy.getObjectName());
                UUIDSerializationUtil.writeUUID(out, proxy.getSource());
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int len = in.readInt();
        if (len > 0) {
            proxies = new ArrayList<>(len);
            for (int i = 0; i < len; i++) {
                ProxyInfo proxy = new ProxyInfo(in.readString(), in.readString(), UUIDSerializationUtil.readUUID(in));
                proxies.add(proxy);
            }
        }
    }

    @Override
    public int getFactoryId() {
        return SpiDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SpiDataSerializerHook.POST_JOIN_PROXY;
    }

    private class CreateProxyTask implements Runnable {
        private final ProxyRegistry registry;
        private final ProxyInfo proxyInfo;

        CreateProxyTask(ProxyRegistry registry, ProxyInfo proxyInfo) {
            this.registry = registry;
            this.proxyInfo = proxyInfo;
        }

        @Override
        public void run() {
            try {
                registry.createProxy(proxyInfo.getObjectName(), proxyInfo.getSource(), true, true);
            } catch (CacheNotExistsException e) {
                // this can happen when a cache destroy event is received
                // after the cache config is replicated during join (pre-join)
                // but before the cache proxy is created (post-join)
                getLogger().fine("Could not create Cache[" + proxyInfo.getObjectName() + "]. It is already destroyed.", e);
            } catch (ReplicatedMapCantBeCreatedOnLiteMemberException e) {
                // this happens when there is a lite member in the cluster
                // and a data member creates a ReplicatedMap proxy
                // (this is totally expected and doesn't need logging)
                ignore(e);
            } catch (Exception e) {
                logProxyCreationFailure(proxyInfo, e);
            }
        }
    }
}
