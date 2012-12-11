/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.management;

import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Instance;
import com.hazelcast.core.Prefix;
import com.hazelcast.impl.base.DistributedLock;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

//import com.hazelcast.impl.CMap;

public class LockInformationCallable extends ClusterServiceCallable implements
        Callable<Map<String, LockInformationCallable.MapLockState>>, Serializable, HazelcastInstanceAware {

    public Map<String, MapLockState> call() throws Exception {
        final String globalLockMapName = Prefix.HAZELCAST + "Locks";
        final List<String> mapNames = new ArrayList<String>();
        mapNames.add(globalLockMapName);
        for (Instance instance : hazelcastInstance.getInstances()) {
            if (instance.getInstanceType().isMap()) {
                IMap imap = (IMap) instance;
                mapNames.add(imap.getName());
            }
        }
        final ConcurrentHashMap<String, MapLockState> lockInformation = new ConcurrentHashMap<String, MapLockState>();
//        getClusterService().enqueueAndWait(new Processable() {
//            public void process() {
//                for (String mapName : mapNames) {
//                    final CMap cmap = getCMap(mapName);
//                    Map<Object, DistributedLock> lockOwners = new HashMap<Object, DistributedLock>();
//                    Map<Object, DistributedLock> lockRequested = new HashMap<Object, DistributedLock>();
//                    cmap.collectScheduledLocks(lockOwners, lockRequested);
//                    MapLockState mapLockState = new MapLockState();
//                    mapLockState.setLockOwners(lockOwners);
//                    mapLockState.setLockRequested(lockRequested);
//                    mapLockState.setGlobalLock(mapName.equals(globalLockMapName));
//                    mapLockState.setMapName(mapName);
//                    lockInformation.put(mapName, mapLockState);
//                }
//            }
//        }, 5);
        return lockInformation;
    }

    public static class MapLockState implements java.io.Serializable {
        private boolean globalLock = false;
        private Map<Object, DistributedLock> lockOwners;
        private Map<Object, DistributedLock> lockRequested;
        private String mapName;

        public boolean isGlobalLock() {
            return globalLock;
        }

        public void setGlobalLock(boolean globalLock) {
            this.globalLock = globalLock;
        }

        public Map<Object, DistributedLock> getLockOwners() {
            return lockOwners;
        }

        public void setLockOwners(Map<Object, DistributedLock> lockOwners) {
            this.lockOwners = lockOwners;
        }

        public Map<Object, DistributedLock> getLockRequested() {
            return lockRequested;
        }

        public void setLockRequested(Map<Object, DistributedLock> lockRequested) {
            this.lockRequested = lockRequested;
        }

        public String getMapName() {
            return mapName;
        }

        public void setMapName(String mapName) {
            this.mapName = mapName;
        }
    }
}
