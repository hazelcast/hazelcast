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

import com.hazelcast.logging.ILogger;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

public class ProxyHelper {

    private final static AtomicLong callIdGen = new AtomicLong(0);
    protected final HazelcastClient client;

    final ILogger logger = com.hazelcast.logging.Logger.getLogger(this.getClass().getName());

    public ProxyHelper(HazelcastClient client) {
        this.client = client;
    }

    public int getCurrentThreadId() {
        return (int) Thread.currentThread().getId();
    }


    public static Long newCallId() {
        return callIdGen.incrementAndGet();
    }


    public void sendCall(final Call c) {
        if (c == null) {
            throw new NullPointerException();
        }
        client.getOutRunnable().enQueue(c);
    }

    protected Object doCall(Call c) {
        sendCall(c);
        c.sent = System.nanoTime();
        final int timeout = 5;
        for (int i = 0; ; i++) {
            final Object response = c.getResponse(timeout, TimeUnit.SECONDS);
            if (response != null) {
                c.replied = System.nanoTime();
                return response;
            }
            if (i > 0) {
                logger.log(Level.INFO, "There is no response for " + c
                        + " in " + (timeout * i) + " seconds.");
            }
            if (!client.isActive()) {
                throw new RuntimeException("HazelcastClient is no longer active.");
            }
        }
    }

}
