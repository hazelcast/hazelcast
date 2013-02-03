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

package com.hazelcast.client.impl;

import com.hazelcast.client.Call;
import com.hazelcast.client.ClientRunnable;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.Packet;
import com.hazelcast.core.Instance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static com.hazelcast.client.IOUtil.toObject;
import static com.hazelcast.impl.BaseManager.getInstanceType;

public class ListenerManager extends ClientRunnable {

    final private ILogger logger = Logger.getLogger(this.getClass().getName());
    final private HazelcastClient client;
    final private BlockingQueue<Object> queue = new LinkedBlockingQueue<Object>();
    final private InstanceListenerManager instanceListenerManager;
    final private MembershipListenerManager membershipListenerManager;
    final private MessageListenerManager messageListenerManager;
    final private EntryListenerManager entryListenerManager;
    final private ItemListenerManager itemListenerManager;
    final private QueueItemListenerManager queueItemListenerManager;

    public ListenerManager(HazelcastClient hazelcastClient) {
        this.client = hazelcastClient;
        instanceListenerManager = new InstanceListenerManager(client);
        membershipListenerManager = new MembershipListenerManager(client);
        messageListenerManager = new MessageListenerManager();
        entryListenerManager = new EntryListenerManager();
        itemListenerManager = new ItemListenerManager(entryListenerManager);
        queueItemListenerManager = new QueueItemListenerManager();
    }

    public void enqueue(Object object) {
        try {
            queue.put(object);
        } catch (InterruptedException e) {
            logger.log(Level.FINEST, e.getMessage(), e);
        }
    }

    protected void customRun() throws InterruptedException {
        try {
            Object obj = queue.poll(100, TimeUnit.MILLISECONDS);
            if (obj == null) {
                return;
            }
            if (obj instanceof Packet) {
                Packet packet = (Packet) obj;
                if (packet.getName() == null) {
                    Object eventType = toObject(packet.getValue());
                    if (new Integer(0).equals(eventType) || new Integer(2).equals(eventType)) {
                        instanceListenerManager.notifyListeners(packet);
                    } else {
                        membershipListenerManager.notifyListeners(packet);
                    }
                } else if (getInstanceType(packet.getName()).equals(Instance.InstanceType.TOPIC)) {
                    messageListenerManager.notifyMessageListeners(packet);
                } else if (getInstanceType(packet.getName()).equals(Instance.InstanceType.QUEUE)) {
                    queueItemListenerManager.notifyListeners(packet);
                } else {
                    entryListenerManager.notifyListeners(packet);
                }
            } else if (obj instanceof Runnable) {
                ((Runnable) obj).run();
            }
        } catch (InterruptedException ine) {
            throw ine;
        } catch (Throwable ignored) {
            ignored.printStackTrace();
        }
    }

    public Collection<Call> getListenerCalls() {
        final List<Call> calls = new ArrayList<Call>();
        calls.addAll(instanceListenerManager.calls(client));
        calls.addAll(entryListenerManager.calls(client));
        calls.addAll(itemListenerManager.calls(client));
        calls.addAll(queueItemListenerManager.calls(client));
        calls.addAll(messageListenerManager.calls(client));
        return calls;
    }

    public InstanceListenerManager getInstanceListenerManager() {
        return instanceListenerManager;
    }

    public MembershipListenerManager getMembershipListenerManager() {
        return membershipListenerManager;
    }

    public MessageListenerManager getMessageListenerManager() {
        return messageListenerManager;
    }

    public EntryListenerManager getEntryListenerManager() {
        return entryListenerManager;
    }

    public ItemListenerManager getItemListenerManager() {
        return itemListenerManager;
    }

    public QueueItemListenerManager getQueueItemListenerManager() {
        return queueItemListenerManager;
    }
}
