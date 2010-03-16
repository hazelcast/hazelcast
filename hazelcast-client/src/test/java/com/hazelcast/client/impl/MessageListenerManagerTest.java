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

import com.hazelcast.client.Packet;
import com.hazelcast.core.MessageListener;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.client.Serializer.toByte;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MessageListenerManagerTest {
    @Test
    public void testRegisterMessageListener() throws Exception {
        MessageListenerManager manager = new MessageListenerManager();
        String name = "default";
        assertTrue(manager.noMessageListenerRegistered(name));
        MessageListener listener = new MessageListener() {

            public void onMessage(Object message) {
            }
        };
        manager.registerMessageListener(name, listener);
        assertFalse(manager.noMessageListenerRegistered(name));
    }

    @Test
    public void testRemoveMessageListener() throws Exception {
        MessageListenerManager manager = new MessageListenerManager();
        String name = "default";
        assertTrue(manager.noMessageListenerRegistered(name));
        MessageListener listener = new MessageListener() {

            public void onMessage(Object message) {
            }
        };
        manager.registerMessageListener(name, listener);
        assertFalse(manager.noMessageListenerRegistered(name));
        manager.removeMessageListener(name, listener);
        assertTrue(manager.noMessageListenerRegistered(name));
        manager.removeMessageListener(name, listener);
        assertTrue(manager.noMessageListenerRegistered(name));
    }

    @Test
    public void testNotifyMessageListeners() throws Exception {
        final MessageListenerManager manager = new MessageListenerManager();
        final String name = "default";
        assertTrue(manager.noMessageListenerRegistered(name));
        final String myMessage = "my myMessage";
        final CountDownLatch latch = new CountDownLatch(1);
        MessageListener listener = new MessageListener() {

            public void onMessage(Object message) {
                if (message.equals(myMessage)) {
                    latch.countDown();
                }
            }
        };
        manager.registerMessageListener(name, listener);
        assertFalse(manager.noMessageListenerRegistered(name));
        new Thread(new Runnable() {
            public void run() {
                Packet packet = new Packet();
                packet.setName(name);
                packet.setKey(toByte(myMessage));
                manager.notifyMessageListeners(packet);
            }
        }).start();

        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }
}
