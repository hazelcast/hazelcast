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

package com.hazelcast.client;

import com.hazelcast.core.ILock;
import com.hazelcast.monitor.LocalLockStats;
import com.hazelcast.nio.Protocol;
import com.hazelcast.nio.protocol.Command;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import static com.hazelcast.client.ProxyHelper.check;

public class LockClientProxy implements ILock {
    final ProxyHelper proxyHelper;
    final Object lockObject;

    public LockClientProxy(Object object, HazelcastClient client) {
        proxyHelper = new ProxyHelper(client.getSerializationService(), client.getConnectionPool());
        lockObject = object;
        check(lockObject);
    }

    public Object getLockObject() {
        return lockObject;
    }

    public void lockInterruptibly() throws InterruptedException {
        throw new UnsupportedOperationException("Is not implemented!");
    }

    public void lock() {
        proxyHelper.doCommand(Command.LOCK,new String[]{}, proxyHelper.toData(lockObject));
    }

    public boolean isLocked() {
        Protocol protocol = proxyHelper.doCommand(Command.ISLOCKED, new String[]{}, proxyHelper.toData(lockObject));
        return Boolean.valueOf(protocol.args[0]);
    }

    public boolean tryLock() {
        Protocol protocol = proxyHelper.doCommand(Command.TRYLOCK, new String[]{}, proxyHelper.toData(lockObject));
        return Boolean.valueOf(protocol.args[0]);
    }

    public boolean tryLock(long time, TimeUnit timeunit) {
        Protocol protocol = proxyHelper.doCommand(Command.TRYLOCK, new String[]{String.valueOf(timeunit.toMillis(time))}, proxyHelper.toData(lockObject));
        return Boolean.valueOf(protocol.args[0]);
    }

    public void unlock() {
        proxyHelper.doCommand(Command.UNLOCK,new String[]{}, proxyHelper.toData(lockObject));
    }

    public void forceUnlock() {
        proxyHelper.doCommand(Command.FORCEUNLOCK,new String[]{}, proxyHelper.toData(lockObject));
    }

    public Condition newCondition() {
        return null;
    }

    public void destroy() {
        proxyHelper.doCommand(Command.DESTROY, "lock", proxyHelper.toData(lockObject));
    }

    public Object getId() {
        return null;
    }

    public String getName() {
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (o != null && o instanceof ILock) {
            return getId().equals(((ILock) o).getId());
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return getId().hashCode();
    }

    public LocalLockStats getLocalLockStats() {
        throw new UnsupportedOperationException();
    }
}
