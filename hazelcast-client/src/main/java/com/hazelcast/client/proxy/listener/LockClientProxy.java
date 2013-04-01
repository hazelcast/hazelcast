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

package com.hazelcast.client.proxy.listener;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.proxy.ProxyHelper;
import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.core.ICondition;
import com.hazelcast.core.ILock;
import com.hazelcast.monitor.LocalLockStats;
import com.hazelcast.nio.protocol.Command;
import com.hazelcast.nio.serialization.Data;
import com.sun.tools.javac.comp.Todo;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

public class LockClientProxy implements ILock {

    final HazelcastClient client;
    final Object key;
    final Data dKey;
    private final ProxyHelper proxyHelper;

    public LockClientProxy(HazelcastClient hazelcastClient, Object lock) {
        this.client = hazelcastClient;
        this.key = lock;
        proxyHelper = new ProxyHelper(client);
        dKey = proxyHelper.toData(key);
    }

    @Override
    public void lock() {
        proxyHelper.doCommand(dKey, Command.LOCK, null, dKey);
    }

    @Override
    public void lock(long leaseTime, TimeUnit timeUnit) {
        // todo to be implemented
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
    }

    @Override
    public boolean tryLock() {
        return proxyHelper.doCommandAsBoolean(dKey, Command.TRYLOCK, null, dKey);
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        return proxyHelper.doCommandAsBoolean(dKey, Command.TRYLOCK, new String[]{String.valueOf(unit.toMillis(time))}, dKey);
    }

    @Override
    public void unlock() {
        proxyHelper.doCommand(dKey, Command.LOCK, null, dKey);
    }

    @Override
    public Condition newCondition() {
        return null;
    }

    @Override
    public Object getKey() {
        return key;
    }

    @Override
    public LocalLockStats getLocalLockStats() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isLocked() {
        return proxyHelper.doCommandAsBoolean(dKey, Command.ISLOCKED, null, dKey);
    }

    @Override
    public void forceUnlock() {
        proxyHelper.doCommand(dKey, Command.FORCEUNLOCK, null, dKey);
    }

    @Override
    public ICondition newCondition(String name) {
        return null;
    }

    @Override
    public Object getId() {
        return dKey;
    }

    @Override
    public String getName() {
        return String.valueOf(key);
    }

    @Override
    public void destroy() {
        proxyHelper.doCommand(Command.DESTROY, new String[]{LockService.SERVICE_NAME, getName()});
    }
}
