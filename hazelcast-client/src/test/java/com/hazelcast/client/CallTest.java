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

package com.hazelcast.client;

import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class CallTest {
    @Test
    public void testGetResponse() throws Exception {
        Object response = new Object();
        Packet request = new Packet();
        long callId = 1;
        request.setCallId(callId);
        Call call = new Call(callId, request);
        call.setResponse(response);
        Object realResponse = call.getResponse();
        assertEquals(response, realResponse);
        assertEquals(callId, request.getCallId());
    }

    @Test(expected = RuntimeException.class)
    public void testGetResponseAsException() throws Exception {
        Object response = new RuntimeException();
        Packet request = new Packet();
        long callId = 1;
        request.setCallId(callId);
        Call call = new Call(callId, request);
        call.setResponse(response);
        Object realResponse = call.getResponse();
        assertEquals(response, realResponse);
        assertEquals(callId, request.getCallId());
    }

    @Test
    public void testGetResponseOnTime() throws Exception {
        final Object response = new Object();
        Packet request = new Packet();
        long callId = 1;
        request.setCallId(callId);
        final Call call = new Call(callId, request);
        new Thread(new Runnable() {
            public void run() {
                call.setResponse(response);
            }
        }).start();
        Object realResponse = call.getResponse(1, TimeUnit.SECONDS);
        assertEquals(response, realResponse);
        assertEquals(callId, request.getCallId());
    }

    @Test
    public void testGetResponseNotOnTime() throws Exception {
        final Object response = new Object();
        Packet request = new Packet();
        long callId = 1;
        request.setCallId(callId);
        final Call call = new Call(callId, request);
        new Thread(new Runnable() {
            public void run() {
                try {
                    Thread.sleep(1500);
                } catch (InterruptedException e) {
                }
                call.setResponse(response);
            }
        }).start();
        Object realResponse = call.getResponse(1, TimeUnit.SECONDS);
        assertNull(realResponse);
        assertEquals(callId, request.getCallId());
    }
}
