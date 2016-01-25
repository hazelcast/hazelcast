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

package com.hazelcast.spi.impl.proxyservice.impl.operations;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.InitializingObject;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.proxyservice.impl.ProxyInfo;
import com.hazelcast.spi.impl.proxyservice.impl.ProxyRegistry;
import com.hazelcast.spi.impl.proxyservice.impl.ProxyServiceImpl;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class PostJoinProxyOperation extends AbstractOperation {

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
                executionService.execute(ExecutionService.SYSTEM_EXECUTOR, new Runnable() {
                    @Override
                    public void run() {
                        registry.createProxy(proxy.getObjectName(), false, true);
                    }
                });
            } catch (Throwable t) {
                getLogger().warning("Cannot create proxy [" + proxy.getServiceName() + ":"
                        + proxy.getObjectName() + "]!", t);
            }
        }
    }

    @Override
    public String getServiceName() {
        return ProxyServiceImpl.SERVICE_NAME;
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        int len = proxies != null ? proxies.size() : 0;
        out.writeInt(len);
        if (len > 0) {
            for (ProxyInfo proxy : proxies) {
                out.writeUTF(proxy.getServiceName());
                out.writeObject(proxy.getObjectName());
                // writing as object for backward-compatibility
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int len = in.readInt();
        if (len > 0) {
            proxies = new ArrayList<ProxyInfo>(len);
            for (int i = 0; i < len; i++) {
                ProxyInfo proxy = new ProxyInfo(in.readUTF(), (String) in.readObject());
                proxies.add(proxy);
            }
        }
    }

    private class InitializeRunnable implements Runnable {
        private final DistributedObject object;

        public InitializeRunnable(DistributedObject object) {
            this.object = object;
        }

        @Override
        public void run() {
            try {
                ((InitializingObject) object).initialize();
            } catch (Exception e) {
                getLogger().warning("Error while initializing proxy: " + object, e);
            }
        }
    }
}
