/*
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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

import static com.hazelcast.client.Serializer.toObject;

import java.util.concurrent.*;

public class FutureProxy<T> implements Future<T> {
    final Callable<T> callable;
    final ProxyHelper proxyHelper;
    volatile boolean isDone = false;
    private T result;
    BlockingQueue<Packet> queue = new LinkedBlockingQueue<Packet>();


    public FutureProxy(ProxyHelper proxyHelper, Callable<T> callable){
        this.proxyHelper = proxyHelper;
        this.callable = callable;
    }

    public boolean cancel(boolean b) {
        return false;
    }

    public boolean isCancelled() {
        return false;
    }

    public boolean isDone() {
        return isDone;
    }

    public T get() throws InterruptedException, ExecutionException {
        try {
            return get(-1, null);
        } catch (TimeoutException ignore) {
            return null;
        }

    }

    public T get(long l, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException {
        if(result == null){
            synchronized (this){
                if(result==null){
                    Packet packet = null;
                    if(l<0){
                        packet = queue.take();
                    }
                    else{
                        packet = queue.poll(l, timeUnit);
                    }
                    result = handleResult(packet);
                }
            }
        }
        return result;


    }

    private T handleResult(Packet packet) throws ExecutionException {
        Object o = toObject(packet.getValue());
        if(o instanceof ExecutionException){
            throw (ExecutionException) o;
        }
        else{
            return (T)o;
        }
    }

    public void enqueue(Packet packet){
        queue.offer(packet);
        isDone = true;
    }
}
