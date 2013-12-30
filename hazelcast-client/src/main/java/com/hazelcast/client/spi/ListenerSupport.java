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

import com.hazelcast.client.util.ErrorHandler;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.spi.Callback;
import com.hazelcast.util.UuidUtil;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author ali 5/20/13
 */
public final class ListenerSupport  {

    private final ClientContext context;
    private final EventHandler handler;
    private final Object registrationRequest;
    private Future<?> future;
    private volatile boolean active = true;
    private volatile ResponseStream lastStream;
    private Object partitionKey;
    final CountDownLatch latch = new CountDownLatch(1);


    public ListenerSupport(ClientContext context, Object registrationRequest, EventHandler handler, Object partitionKey) {
        this.context = context;
        this.registrationRequest = registrationRequest;
        this.handler = handler;
        this.partitionKey = partitionKey;
    }

    public String listen() {
        return listen(null);
    }

    public String listen(final Callback<Exception> callback){
        future = context.getExecutionService().submit(new Runnable() {
            public void run() {
                while (active && !Thread.currentThread().isInterrupted()) {
                    try {
                        EventResponseHandler eventResponseHandler = new EventResponseHandler();
                        if (partitionKey == null){
                            context.getInvocationService().invokeOnRandomTarget(registrationRequest, eventResponseHandler);
                        } else {
                            context.getInvocationService().invokeOnKeyOwner(registrationRequest, partitionKey, eventResponseHandler);
                        }
                    } catch (Exception e) {
                        if (callback != null) {
                            callback.notify(e);
                        }
                        if (e instanceof HazelcastInstanceNotActiveException){
                            try {
                                Thread.sleep(context.getClientConfig().getConnectionAttemptPeriod());
                            } catch (InterruptedException ignored) {
                            }
                        }
                    }
                }
            }
        });
        try {
            if(!latch.await(1, TimeUnit.MINUTES)){
                throw new HazelcastException("Could not register listener!!!");
            }
        } catch (InterruptedException ignored) {
        }
        return UuidUtil.buildRandomUuidString();
    }

    public void stop() {
        active = false;
        if (future != null) {
            future.cancel(true);
        }
        final ResponseStream s = lastStream;
        if (s != null) {
            try {
                s.end();
            } catch (IOException ignored) {
            }
        }
    }

    private class EventResponseHandler implements ResponseHandler {

        public void handle(final ResponseStream stream) throws Exception {
            try {
                stream.read(); // initial ok response
                lastStream = stream;
                latch.countDown();
                while (active && !Thread.currentThread().isInterrupted()) {
                    final Object event = stream.read();
                    handler.handle(event);
                }
            } catch (Exception e) {
                try {
                    stream.end();
                } catch (IOException ignored) {
                }
                if (ErrorHandler.isRetryable(e)) {
                    throw e;
                } else {
                    active = false;
                }
            }

        }
    }

}
