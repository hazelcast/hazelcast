/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.client.impl;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.Packet;
import com.hazelcast.core.Instance;
import com.hazelcast.core.InstanceEvent;
import com.hazelcast.core.InstanceListener;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.hazelcast.client.Serializer.toObject;

public class InstanceListenerManager {
    final private List<InstanceListener> instanceListeners = new CopyOnWriteArrayList<InstanceListener>();
    final private HazelcastClient client;

    public InstanceListenerManager(HazelcastClient client) {
        this.client = client;
    }

    public void registerInstanceListener(InstanceListener listener) {
        this.instanceListeners.add(listener);
    }

    public void removeInstanceListener(InstanceListener instanceListener) {
        this.instanceListeners.remove(instanceListener);
    }

    public synchronized boolean noInstanceListenerRegistered() {
        return instanceListeners.isEmpty();
    }

    public void notifyInstanceListeners(Packet packet) {
        String id = (String) toObject(packet.getKey());
        InstanceEvent.InstanceEventType instanceEventType = (InstanceEvent.InstanceEventType) toObject(packet.getValue());
        InstanceEvent event = new InstanceEvent(instanceEventType, (Instance) client.getClientProxy(id));
        for (Iterator<InstanceListener> it = instanceListeners.iterator(); it.hasNext();) {
            InstanceListener listener = it.next();
            if (InstanceEvent.InstanceEventType.CREATED.equals(event.getEventType())) {
                listener.instanceCreated(event);
            } else if (InstanceEvent.InstanceEventType.DESTROYED.equals(event.getEventType())) {
                listener.instanceDestroyed(event);
            }
        }
    }
}
