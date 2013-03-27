///*
//* Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
//*
//* Licensed under the Apache License, Version 2.0 (the "License");
//* you may not use this file except in compliance with the License.
//* You may obtain a copy of the License at
//*
//* http://www.apache.org/licenses/LICENSE-2.0
//*
//* Unless required by applicable law or agreed to in writing, software
//* distributed under the License is distributed on an "AS IS" BASIS,
//* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//* See the License for the specific language governing permissions and
//* limitations under the License.
//*/
//
//package com.hazelcast.management;
//
//import com.hazelcast.core.DistributedObject;
//import com.hazelcast.core.HazelcastInstanceAware;
//import com.hazelcast.core.ILock;
//import com.hazelcast.core.IMap;
//import com.hazelcast.map.Record;
//
//import java.io.Serializable;
//import java.util.*;
//import java.util.concurrent.Callable;
//import java.util.concurrent.ConcurrentHashMap;
//
////import com.hazelcast.impl.CMap;
//
//public class LockInformationCallable extends ClusterServiceCallable implements
//        Callable<Map<String, LockInformationCallable.MapLockState>>, Serializable, HazelcastInstanceAware {
//
//    public Map<String, MapLockState> call() throws Exception {
//        final String globalLockMapName = "hazelcast:Locks";
//        final List<String> mapNames = new ArrayList<String>();
//        mapNames.add(globalLockMapName);
//        for (DistributedObject distributedObject : hazelcastInstance.getDistributedObjects()) {
//            if (distributedObject instanceof IMap) {
//                IMap imap = (IMap) distributedObject;
//                mapNames.add(imap.getName());
//            }
//        }
//        final ConcurrentHashMap<String, MapLockState> lockInformation = new ConcurrentHashMap<String, MapLockState>();
////        getClusterService().enqueueAndWait(new Processable() {      //TODO @msk ???
////            public void process() {
//                for (String mapName : mapNames) {
//                    final IMap map = getMap(mapName);
//                    Map<Object, ILock> lockOwners = new HashMap<Object, ILock>();
//                    Map<Object, ILock> lockRequested = new HashMap<Object, ILock>();
//                    collectScheduledLocks(map, lockOwners, lockRequested);
//                    MapLockState mapLockState = new MapLockState();
//                    mapLockState.setLockOwners(lockOwners);
//                    mapLockState.setLockRequested(lockRequested);
//                    mapLockState.setGlobalLock(mapName.equals(globalLockMapName));
//                    mapLockState.setMapName(mapName);
//                    lockInformation.put(mapName, mapLockState);
//                }
////            }
////        }, 5);
//        return lockInformation;
//    }
//
//    private void collectScheduledLocks(IMap map ,Map<Object, ILock> lockOwners ,Map<Object, ILock> lockRequested){
//        Collection<Record> records = map.values();
//        for (Record record : records) {
////            ILock dLock = record.getLock();             //TODO @msk ???
////            if (dLock != null && dLock.isLocked()) {
////                lockOwners.put(record.getKey(), dLock);
////                List<ScheduledAction> scheduledActions = record.getScheduledActions();
////                if (scheduledActions != null) {
////                    for (ScheduledAction scheduledAction : scheduledActions) {
////                        Request request = scheduledAction.getRequest();
////                        if (ClusterOperation.CONCURRENT_MAP_LOCK.equals(request.operation)) {
////                            lockRequested.put(record.getKey(),
////                                    new ILock(request.lockAddress, request.lockThreadId));
////                        }
////                    }
////                }
////            }
//        }
//    }
//
//    public static class MapLockState implements java.io.Serializable {
//        private boolean globalLock = false;
//        private Map<Object, ILock> lockOwners;
//        private Map<Object, ILock> lockRequested;
//        private String mapName;
//
//        public boolean isGlobalLock() {
//            return globalLock;
//        }
//
//        public void setGlobalLock(boolean globalLock) {
//            this.globalLock = globalLock;
//        }
//
//        public Map<Object, ILock> getLockOwners() {
//            return lockOwners;
//        }
//
//        public void setLockOwners(Map<Object, ILock> lockOwners) {
//            this.lockOwners = lockOwners;
//        }
//
//        public Map<Object, ILock> getLockRequested() {
//            return lockRequested;
//        }
//
//        public void setLockRequested(Map<Object, ILock> lockRequested) {
//            this.lockRequested = lockRequested;
//        }
//
//        public String getMapName() {
//            return mapName;
//        }
//
//        public void setMapName(String mapName) {
//            this.mapName = mapName;
//        }
//    }
//}
