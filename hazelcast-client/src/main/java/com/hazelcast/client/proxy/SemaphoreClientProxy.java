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

package com.hazelcast.client.proxy;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.concurrent.semaphore.SemaphoreService;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.monitor.LocalSemaphoreStats;
import com.hazelcast.deprecated.nio.protocol.Command;

import java.util.concurrent.TimeUnit;

public class SemaphoreClientProxy implements ISemaphore {

    private final String name;
    private final ProxyHelper proxyHelper;

    public SemaphoreClientProxy(HazelcastClient client, String name) {
        this.name = name;
        proxyHelper = new ProxyHelper(client);
    }

    public void acquire() throws InterruptedException {
        acquire(1);
    }

    public void acquire(int permits) throws InterruptedException {
        if (Thread.currentThread().isInterrupted())
            throw new InterruptedException();
        proxyHelper.doCommand(Command.SEMACQUIRE, new String[]{getName(), String.valueOf(permits)});
    }

    public int availablePermits() {
        return proxyHelper.doCommandAsInt(Command.SEMAVAILABLEPERMITS, new String[]{getName()});
    }

    public int drainPermits() {
        return proxyHelper.doCommandAsInt(Command.SEMDRAINPERMITS, new String[]{getName()});
    }

    public void reducePermits(int permits) {
        proxyHelper.doCommand(Command.SEMREDUCEPERMITS, new String[]{getName(), String.valueOf(permits)});
    }

    public void release() {
        release(1);
    }

    public void release(int permits) {
        proxyHelper.doCommand(Command.SEMRELEASE, new String[]{getName(), String.valueOf(permits)});
    }

    public boolean tryAcquire() {
        return tryAcquire(1);
    }

    public boolean tryAcquire(int permits) {
        try {
            return tryAcquire(permits, 0, TimeUnit.MILLISECONDS);
        } catch (Throwable e) {
            return false;
        }
    }

    public boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
        return tryAcquire(1, timeout, unit);
    }

    public boolean tryAcquire(int permits, long timeout, TimeUnit unit) throws InterruptedException {
        return proxyHelper.doCommandAsBoolean(Command.SEMTRYACQUIRE, new String[]{getName(), String.valueOf(permits), String.valueOf(unit.toMillis(timeout))});
    }

    public void destroy() {
        proxyHelper.doCommand(Command.DESTROY, new String[]{SemaphoreService.SERVICE_NAME, getName()});
    }

    public Object getId() {
        return name;
    }

    public String getName() {
        return name;
    }

    public boolean init(int permits) {
        return proxyHelper.doCommandAsBoolean(Command.SEMINIT, new String[]{getName(), String.valueOf(permits)});
    }
  
    public LocalSemaphoreStats getLocalSemaphoreStats() {
        throw new UnsupportedOperationException();
    }
}
