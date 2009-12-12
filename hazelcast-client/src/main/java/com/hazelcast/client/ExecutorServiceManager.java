/* 
 * Copyright (c) 2008-2009, Hazel Ltd. All Rights Reserved.
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

import com.hazelcast.impl.ClusterOperation;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Map;

public class ExecutorServiceManager extends ClientRunnable{
    BlockingQueue<FutureProxy> queue = new LinkedBlockingQueue<FutureProxy>();
    AtomicLong executerId = new AtomicLong(0);
    Map<Long, FutureProxy> map = new ConcurrentHashMap<Long, FutureProxy>();
    private final HazelcastClient client;

    public ExecutorServiceManager(HazelcastClient hazelcastClient) {
        this.client = hazelcastClient;
    }


    protected void customRun() throws InterruptedException {
		FutureProxy future = queue.poll(100, TimeUnit.MILLISECONDS);
		if(future==null){
			return;
		}
        sendToExecute(future);
    }

    public void enqueue(FutureProxy<?> future){
        queue.offer(future);
    }


    public void sendToExecute(FutureProxy<?> future) {
        long id = executerId.incrementAndGet();
        Packet request = future.proxyHelper.prepareRequest(ClusterOperation.REMOTELY_EXECUTE, future.callable, null);
        request.setLongValue(id);
        Call c = future.proxyHelper.createCall(request);
        map.put(id, future);
        client.out.enQueue(c);

    }

    public void enqueue(Packet packet) {
        FutureProxy future = map.get(packet.getLongValue());
        if(future!=null){
            future.enqueue(packet);
        }
        map.remove(packet.getLongValue());
    }
}
