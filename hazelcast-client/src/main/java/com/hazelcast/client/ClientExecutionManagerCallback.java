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

import com.hazelcast.impl.ExecutionManagerCallback;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.client.Serializer.toObject;

public abstract class ClientExecutionManagerCallback implements ExecutionManagerCallback {
    final BlockingQueue<Packet> queue = new LinkedBlockingQueue<Packet>();

    public ClientExecutionManagerCallback() {
    }

    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    public void get() throws InterruptedException, ExecutionException {
        get(-1, null);
    }

    //!!called by multiple threads
    public abstract void get(long l, TimeUnit timeUnit) throws InterruptedException, ExecutionException;

    protected Object handleResult(Packet packet) throws ExecutionException {
        return toObject(packet.getValue());
    }

    public void offer(Packet packet) {
        queue.offer(packet);
    }

    public static class SingleResultClientExecutionManagerCallBack extends ClientExecutionManagerCallback {
        private volatile Object result;
        private volatile boolean done = false;

        @Override
        public void get(long l, TimeUnit timeUnit) throws InterruptedException {
//            if (done) {
//                return result;
//            }
//            synchronized (this) {
//                if (!done) {
//                    Packet packet;
//                    if (l < 0) {
//                        packet = queue.take();
//                    } else {
//                        packet = queue.poll(l, timeUnit);
//                    }
//                    try {
//                        this.result = handleResult(packet);
//                    } catch (ExecutionException e) {
//                        throw new RuntimeException(e);
//                    }
//                    done = true;
//                }
//                return result;
//            }
        }
    }

    public static class MultipleResultClientExecutionManagerCallBack extends ClientExecutionManagerCallback {
        private volatile Collection<Object> result;
        private volatile Iterator<Object> it;

        @Override
        public void get(long l, TimeUnit timeUnit) throws InterruptedException, ExecutionException {
//            if (result == null) {
//                synchronized (this) {
//                    if (result == null) {
//                        Packet packet;
//                        if (l < 0) {
//                            packet = queue.take();
//                        } else {
//                            packet = queue.poll(l, timeUnit);
//                        }
//                        try {
//                            this.result = handleResult(packet);
//                        } catch (ExecutionException e) {
//                            throw e;
//                        }
//                    }
//                    if (it == null) {
//                        it = result.iterator();
//                    }
//                }
//            }
//            if (it.hasNext()) {
//                return it.next();
//            } else {
//                return OBJECT_DONE;
//            }
        }

        protected Collection<Object> handleResult(Packet packet) throws ExecutionException {
            Object o = toObject(packet.getValue());
            if (o instanceof ExecutionException) {
                ExecutionException e = (ExecutionException) o;
                throw e;
            }
            if (o instanceof Collection) {
                return (Collection) o;
            }
            throw new RuntimeException("Should return collection, but returned object is: " + o);
        }
    }
}
