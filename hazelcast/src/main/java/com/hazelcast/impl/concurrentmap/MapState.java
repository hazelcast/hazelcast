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
//import com.hazelcast.nio.DataSerializable;
//
//import java.io.DataInput;
//import java.io.DataOutput;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//
//class MapState implements DataSerializable {
//    String name;
//    List<AddMapIndex> lsMapIndexes = new ArrayList<AddMapIndex>();
//
//    MapState() {
//    }
//
//    MapState(String name) {
//        this.name = name;
//    }
//
//    void addMapIndex(AddMapIndex mapIndex) {
//        lsMapIndexes.add(mapIndex);
//    }
//
//    public void writeData(DataOutput out) throws IOException {
//        out.writeUTF(name);
//        out.writeInt(lsMapIndexes.size());
//        for (AddMapIndex mapIndex : lsMapIndexes) {
//            mapIndex.writeData(out);
//        }
//    }
//
//    public void readData(DataInput in) throws IOException {
//        name = in.readUTF();
//        int size = in.readInt();
//        for (int i = 0; i < size; i++) {
//            AddMapIndex mapIndex = new AddMapIndex();
//            mapIndex.readData(in);
//            lsMapIndexes.add(mapIndex);
//        }
//    }
//}
