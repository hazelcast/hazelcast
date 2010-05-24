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

package com.hazelcast.client;

import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.client.Serializer.toByte;
import static com.hazelcast.client.Serializer.toObject;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MapClientProxyTest {

    @Test
    public void testConditionalPutWithTimeout() throws InterruptedException, IOException {
        HazelcastClient client = mock(HazelcastClient.class);
        Map<Long, Call> callMap = new HashMap<Long, Call>();
        assertTrue(callMap.isEmpty());
        OutRunnable out = new OutRunnable(client, callMap, mock(PacketWriter.class));
        when(client.getOutRunnable()).thenReturn(out);
        ConnectionManager connectionManager = mock(ConnectionManager.class);
        Connection connection = mock(Connection.class);
        when(connectionManager.getConnection()).thenReturn(connection);
        when(client.getConnectionManager()).thenReturn(connectionManager);
        final MapClientProxy<String, String> imap = new MapClientProxy(client, "default");
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch correct = new CountDownLatch(1);
        final long ttl = 10;
        final TimeUnit timeunit = TimeUnit.MILLISECONDS;
        final String key = "1";
        final String value = "value";
        new Thread(new Runnable() {

            public void run() {
                latch.countDown();
                String oldValue = imap.putIfAbsent(key, value, ttl, timeunit);
                if (oldValue.equals("key")) {
                    correct.countDown();
                }
            }
        }).start();
        latch.await();
        Thread.sleep(10);
        out.customRun();
        Call call = callMap.values().iterator().next();
        Packet request = call.getRequest();
        long timeout = request.getTimeout();
        assertEquals(timeout, timeunit.toMillis(ttl));
        assertEquals(key, toObject(request.getKey()));
        assertEquals(value, toObject(request.getValue()));
        Packet packet = new Packet();
        packet.setValue(toByte("key"));
        call.setResponse(packet);
        assertTrue("", correct.await(1, TimeUnit.SECONDS));
    }
}
