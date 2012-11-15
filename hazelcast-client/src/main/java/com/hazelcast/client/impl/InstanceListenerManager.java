/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.client.impl;

import com.hazelcast.client.Call;
import com.hazelcast.client.ClientConfig;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.PacketProxyHelper;
import com.hazelcast.core.InstanceListener;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class InstanceListenerManager {

    final private List<InstanceListener> instanceListeners = new CopyOnWriteArrayList<InstanceListener>();
    final private HazelcastClient client;

    public InstanceListenerManager(HazelcastClient client) {
        this.client = client;
        final ClientConfig clientConfig = client.getClientConfig();
        if (clientConfig != null) {
            for (Object listener : clientConfig.getListeners()) {
                if (listener instanceof InstanceListener) {
                    registerListener((InstanceListener) listener);
                }
            }
        }
    }

    public void registerListener(InstanceListener listener) {
        this.instanceListeners.add(listener);
    }

    public void removeListener(InstanceListener instanceListener) {
        this.instanceListeners.remove(instanceListener);
    }

    public synchronized boolean noListenerRegistered() {
        return instanceListeners.isEmpty();
    }
//    public void notifyListeners(Packet packet) {
//        String id = (String) toObject(packet.getKey());
//        int i = (Integer) toObject(packet.getValue());
//        InstanceEvent.InstanceEventType instanceEventType = (i == 0) ? InstanceEvent.InstanceEventType.CREATED
//                : InstanceEvent.InstanceEventType.DESTROYED;
//        InstanceEvent event = new InstanceEvent(instanceEventType, (Instance) client.getClientProxy(id));
//        for (final InstanceListener listener : instanceListeners) {
//            switch (instanceEventType) {
//                case CREATED:
//                    listener.instanceCreated(event);
//                    break;
//                case DESTROYED:
//                    listener.instanceDestroyed(event);
//                    break;
//                default:
//                    break;
//            }
//        }
//    }

    public Call createNewAddListenerCall(final PacketProxyHelper proxyHelper) {
//        Packet request = proxyHelper.createRequestPacket(ClusterOperation.CLIENT_ADD_INSTANCE_LISTENER, null, null);
//        return proxyHelper.createCall(request);
        return null;
    }

    public Collection<Call> calls(final HazelcastClient client) {
        if (instanceListeners.isEmpty()) {
            return Collections.emptyList();
        }
        return Collections.singletonList(createNewAddListenerCall(new PacketProxyHelper("", client)));
    }
}
