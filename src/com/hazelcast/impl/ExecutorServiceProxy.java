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

package com.hazelcast.impl;

import com.hazelcast.core.DistributedTask;
import com.hazelcast.impl.BaseManager.Processable;
import com.hazelcast.cluster.ClusterService;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;

public class ExecutorServiceProxy implements ExecutorService, Constants {

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    public List invokeAll(Collection tasks) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    public List invokeAll(Collection tasks, long timeout, TimeUnit unit)
            throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    public Object invokeAny(Collection tasks) throws InterruptedException, ExecutionException {
        throw new UnsupportedOperationException();
    }

    public Object invokeAny(Collection tasks, long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        throw new UnsupportedOperationException();
    }

    public boolean isShutdown() {
        throw new UnsupportedOperationException();
    }

    public boolean isTerminated() {
        throw new UnsupportedOperationException();
    }

    public void shutdown() {
        throw new UnsupportedOperationException();
    }

    public List<Runnable> shutdownNow() {
        throw new UnsupportedOperationException();
    }

    public <T> Future<T> submit(Callable<T> task) {
        DistributedTask dtask = new DistributedTask(task);
        Processable action = ExecutorManager.get().createNewExecutionAction(dtask);
        ClusterService clusterService = ClusterService.get();
        clusterService.enqueueAndReturn(action);
        return dtask;
    }

    public Future<?> submit(Runnable task) {
        DistributedTask dtask = null;
        if (task instanceof DistributedTask) {
            dtask = (DistributedTask) task;
        } else {
            dtask = new DistributedTask(task, null);
        }
        Processable action = ExecutorManager.get().createNewExecutionAction(dtask);
        ClusterService clusterService = ClusterService.get();
        clusterService.enqueueAndReturn(action);
        return dtask;
    }

    public <T> Future<T> submit(Runnable task, T result) {
        DistributedTask dtask = null;
        if (task instanceof DistributedTask) {
            dtask = (DistributedTask) task;
        } else {
            dtask = new DistributedTask(task, result);
        }
        Processable action = ExecutorManager.get().createNewExecutionAction(dtask);
        ClusterService clusterService = ClusterService.get();
        clusterService.enqueueAndReturn(action);
        return dtask;
    }

    public void execute(Runnable command) {
        DistributedTask dtask = null;
        if (command instanceof DistributedTask) {
            dtask = (DistributedTask) command;
        } else {
            dtask = new DistributedTask(command, null);
        }
        Processable action = ExecutorManager.get().createNewExecutionAction(dtask);
        ClusterService.get().enqueueAndReturn(action);
    }
}
