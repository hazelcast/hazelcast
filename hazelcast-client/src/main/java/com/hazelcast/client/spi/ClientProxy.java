/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.spi;

import com.hazelcast.client.ClientDestroyRequest;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.util.ExceptionUtil;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author mdogan 5/16/13
 */
public abstract class ClientProxy implements DistributedObject {

    private final String serviceName;

    private final String objectName;

    private volatile ClientContext context;

    private final Map<String, ListenerSupport> listenerSupportMap = new ConcurrentHashMap<String, ListenerSupport>();

    protected ClientProxy(String serviceName, String objectName) {
        this.serviceName = serviceName;
        this.objectName = objectName;
    }

    protected final String listen(Object registrationRequest, Object partitionKey, EventHandler handler){
        ListenerSupport listenerSupport = new ListenerSupport(context, registrationRequest, handler, partitionKey);
        String registrationId = listenerSupport.listen();
        listenerSupportMap.put(registrationId, listenerSupport);
        return registrationId;
    }

    protected final  String listen(Object registrationRequest, EventHandler handler){
        return listen(registrationRequest, null, handler);
    }

    protected final  boolean stopListening(String registrationId){
        final ListenerSupport listenerSupport = listenerSupportMap.remove(registrationId);
        if (listenerSupport != null){
            listenerSupport.stop();
            return true;
        }
        return false;
    }

    protected final ClientContext getContext() {
        final ClientContext ctx = context;
        if (ctx == null) {
            throw new HazelcastInstanceNotActiveException();
        }
        return context;
    }

    final void setContext(ClientContext context) {
        this.context = context;
    }

    @Deprecated
    public final Object getId() {
        return objectName;
    }

    public final String getName() {
        return objectName;
    }

    public String getPartitionKey() {
        return StringPartitioningStrategy.getPartitionKey(getName());
    }

    public final String getServiceName() {
        return serviceName;
    }

    public final void destroy() {
        onDestroy();
        ClientDestroyRequest request = new ClientDestroyRequest(objectName, getServiceName());
        try {
            context.getInvocationService().invokeOnRandomTarget(request);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        context.removeProxy(this);
        context = null;
    }

    protected abstract void onDestroy();
}
