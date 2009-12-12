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
import com.hazelcast.core.DistributedTask;

import java.util.concurrent.*;
import java.util.List;
import java.util.Collection;
import java.util.ArrayList;
import java.io.Serializable;

public class ExecutorServiceClientProxy implements ClientProxy, ExecutorService{

    final ProxyHelper proxyHelper;
	final private HazelcastClient client;

	public ExecutorServiceClientProxy(HazelcastClient client) {
		this.client = client;
		proxyHelper = new ProxyHelper("", client);
	}

    public void setOutRunnable(OutRunnable out) {
        proxyHelper.setOutRunnable(out);
    }

    public void shutdown() {

    }

    public List<Runnable> shutdownNow() {
        return new ArrayList<Runnable>();
    }

    public boolean isShutdown() {
        throw new UnsupportedOperationException();
    }

    public boolean isTerminated() {
        throw new UnsupportedOperationException();
    }

    public boolean awaitTermination(long l, TimeUnit timeUnit) throws InterruptedException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    

    public <T> Future<T> submit(Callable<T> tCallable) {
        check(tCallable);
        FutureProxy<T> future = new FutureProxy(proxyHelper, tCallable);
        client.executorServiceManager.enqueue(future);
        return future;
    }

    private <T> void check(Object o) {
        if (o == null) {
            throw new NullPointerException("Object cannot be null.");
        }
        if (!(o instanceof Serializable)) {
            throw new IllegalArgumentException(o.getClass().getName() + " is not Serializable.");
        }
    }

    public <T> Future<T> submit(Runnable runnable, T t) {
        check(runnable);
        DistributedTask.DistributedRunnableAdapterImpl<T> adapter  = new DistributedTask.DistributedRunnableAdapterImpl(runnable, t);
        return submit(adapter);
    }

    public Future<?> submit(Runnable runnable) {
        return submit(runnable, null);
    }

    public <T> List<Future<T>> invokeAll(Collection<Callable<T>> callables) throws InterruptedException {
        // copied from ExecutorServiceProxy
    	if (callables == null)
    		throw new NullPointerException();
    	List<Future<T>> futures = new ArrayList<Future<T>>(callables.size());
    	boolean done = false;
    	try {
    		for (Callable<T> command : callables) {
    	        futures.add(submit(command));
    		}
    		for (Future<T> f : futures) {
    			if (!f.isDone()) {
    				try {
    					f.get();
    				}
    				catch (CancellationException ignore) {
    				}
    				catch (ExecutionException ignore) {
    				}
    			}
    		}
    		done = true;
    		return futures;
    	}
    	finally {
    		if (!done)
    			for (Future<T> f : futures)
    				f.cancel(true);
    	}
    }

    public <T> List<Future<T>> invokeAll(Collection<Callable<T>> callables, long l, TimeUnit timeUnit) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    public <T> T invokeAny(Collection<Callable<T>> callables) throws InterruptedException, ExecutionException {
        throw new UnsupportedOperationException();
    }

    public <T> T invokeAny(Collection<Callable<T>> callables, long l, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException {
        throw new UnsupportedOperationException();
    }

    public void execute(Runnable runnable) {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
