/*
 *  Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.raft.service.lock;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.util.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * TODO: Javadoc Pending...
 */
public class LockRegistrySnapshot implements IdentifiedDataSerializable {

    private List<RaftLockSnapshot> locks;
    private Map<LockInvocationKey, Long> tryLockTimeouts;
    private Collection<String> destroyedLockNames;

    public LockRegistrySnapshot() {
    }

    LockRegistrySnapshot(Collection<RaftLock> locks, Map<LockInvocationKey, Tuple2<Long, Long>> tryLockTimeouts,
            Set<String> destroyedLockNames) {
        this.locks = new ArrayList<RaftLockSnapshot>(locks.size());
        for (RaftLock lock : locks) {
            this.locks.add(lock.toSnapshot());
        }
        this.tryLockTimeouts = new HashMap<LockInvocationKey, Long>(tryLockTimeouts.size());
        for (Entry<LockInvocationKey, Tuple2<Long, Long>> e : tryLockTimeouts.entrySet()) {
            long timeout = e.getValue().element1;
            this.tryLockTimeouts.put(e.getKey(), timeout);
        }
        this.destroyedLockNames = new ArrayList<String>(destroyedLockNames);
    }

    List<RaftLockSnapshot> getLocks() {
        return locks;
    }

    Map<LockInvocationKey, Long> getTryLockTimeouts() {
        return tryLockTimeouts;
    }

    public Collection<String> getDestroyedLockNames() {
        return destroyedLockNames;
    }

    @Override
    public int getFactoryId() {
        return RaftLockDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftLockDataSerializerHook.LOCK_REGISTRY_SNAPSHOT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(locks.size());
        for (RaftLockSnapshot lock : locks) {
            out.writeObject(lock);
        }
        out.writeInt(tryLockTimeouts.size());
        for (Entry<LockInvocationKey, Long> e : tryLockTimeouts.entrySet()) {
            out.writeObject(e.getKey());
            out.writeLong(e.getValue());
        }
        out.writeInt(destroyedLockNames.size());
        for (String name : destroyedLockNames) {
            out.writeUTF(name);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int lockCount = in.readInt();
        locks = new ArrayList<RaftLockSnapshot>(lockCount);
        for (int i = 0; i < lockCount; i++) {
            RaftLockSnapshot lock = in.readObject();
            locks.add(lock);
        }

        int tryLockTimeoutCount = in.readInt();
        tryLockTimeouts = new HashMap<LockInvocationKey, Long>(tryLockTimeoutCount);
        for (int i = 0; i < tryLockTimeoutCount; i++) {
            LockInvocationKey key = in.readObject();
            long timestamp = in.readLong();
            tryLockTimeouts.put(key, timestamp);
        }

        int destroyedCount = in.readInt();
        destroyedLockNames = new ArrayList<String>(destroyedCount);
        for (int i = 0; i < destroyedCount; i++) {
            destroyedLockNames.add(in.readUTF());
        }
    }
}
