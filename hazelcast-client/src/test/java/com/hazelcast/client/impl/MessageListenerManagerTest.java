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

import com.hazelcast.client.Packet;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.client.IOUtil.toByte;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class MessageListenerManagerTest {
    @Test
    public void testRegisterMessageListener() throws Exception {
        MessageListenerManager manager = new MessageListenerManager();
        String name = "default";
        assertTrue(manager.noListenerRegistered(name));
        MessageListener listener = new MessageListener<Object>() {

            public void onMessage(Message<Object> message) {
            }
        };
        manager.registerListener(name, listener);
        assertFalse(manager.noListenerRegistered(name));
    }

    @Test
    public void testRemoveMessageListener() throws Exception {
        MessageListenerManager manager = new MessageListenerManager();
        String name = "default";
        assertTrue(manager.noListenerRegistered(name));
        MessageListener listener = new MessageListener<Object>() {

            public void onMessage(Message<Object> message) {
            }
        };
        manager.registerListener(name, listener);
        assertFalse(manager.noListenerRegistered(name));
        manager.removeListener(name, listener);
        assertTrue(manager.noListenerRegistered(name));
        manager.removeListener(name, listener);
        assertTrue(manager.noListenerRegistered(name));
    }

    @Test
    public void testNotifyMessageListeners() throws Exception {
        final MessageListenerManager manager = new MessageListenerManager();
        final String name = "default";
        assertTrue(manager.noListenerRegistered(name));
        final String myMessage = "my myMessage";
        final CountDownLatch latch = new CountDownLatch(1);
        MessageListener listener = new MessageListener<Object>() {

            public void onMessage(Message<Object> message) {
                if (message.getMessageObject().equals(myMessage)) {
                    latch.countDown();
                }
            }
        };
        manager.registerListener(name, listener);
        assertFalse(manager.noListenerRegistered(name));
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
