///*
// * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package com.hazelcast.impl.concurrentmap;
//
//import com.hazelcast.core.HazelcastInstance;
//import com.hazelcast.core.HazelcastInstanceAware;
//import com.hazelcast.impl.CMap;
//import com.hazelcast.impl.FactoryImpl;
//import com.hazelcast.nio.DataSerializable;
//
//import java.io.DataInput;
//import java.io.DataOutput;
//import java.io.IOException;
//import java.util.concurrent.Callable;
//
//public class MapSizeCallable implements Callable<Integer>, DataSerializable, HazelcastInstanceAware {
//    private int partitionVersion;
//    private String mapName;
//    private transient HazelcastInstance hazelcast;
//
//    public MapSizeCallable(String mapName, int partitionVersion) {
//        this.mapName = mapName;
//        this.partitionVersion = partitionVersion;
//    }
//
//    public MapSizeCallable() {
//    }
//
//    public Integer call() throws Exception {
//        FactoryImpl factory = (FactoryImpl) hazelcast;
//        CMap cmap = factory.node.concurrentMapManager.getMap(mapName);
//        if (cmap == null) return 0;
//        return cmap.size(partitionVersion);
//    }
//
//    public void writeData(DataOutput out) throws IOException {
//        out.writeUTF(mapName);
//        out.writeInt(partitionVersion);
//    }
//
//    public void readData(DataInput in) throws IOException {
//        mapName = in.readUTF();
//        partitionVersion = in.readInt();
//    }
//
//    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
//        this.hazelcast = hazelcastInstance;
//    }
//}
