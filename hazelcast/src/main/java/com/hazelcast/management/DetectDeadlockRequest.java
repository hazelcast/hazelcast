///*
// * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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
//package com.hazelcast.management;
//
//import com.hazelcast.nio.ObjectDataInput;
//import com.hazelcast.nio.ObjectDataOutput;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//
//
//public class DetectDeadlockRequest implements ConsoleRequest {
//    public int getType() {
//        return ConsoleRequestConstants.REQUEST_TYPE_DETECT_DEADLOCK;
//    }
//
//    public Object readResponse(ObjectDataInput in) throws IOException {
//        int size = in.readInt();
//        List<Edge> list = new ArrayList<Edge>(size);
//        for (int i = 0; i < size; i++) {
//            Edge e = in.readObject();
//            list.add(e);
//        }
//        return list;
//    }
//
//    public void writeResponse(ManagementCenterService mcs, ObjectDataOutput dos) throws Exception {
//        List<Edge> list = mcs.detectDeadlock();
//        dos.writeInt(list == null ? 0 : list.size());
//        for (Edge edge : list) {
//            dos.writeObject(edge);
//        }
//    }
//
//    public void writeData(ObjectDataOutput out) throws IOException {
//    }
//
//    public void readData(ObjectDataInput in) throws IOException {
//    }
//
//    public static class Edge implements java.io.Serializable {
//        Vertex from;
//        Vertex to;
//        Object key;
//        String mapName;
//        boolean globalLock;
//
//        @Override
//        public String toString() {
//            return "Edge{" +
//                    "to=" + to.owner.getLockAddress() + to.owner.getLockThreadId() +
//                    ", key=" + key +
//                    '}';
//        }
//
//        public boolean isGlobalLock() {
//            return globalLock;
//        }
//
//        public void setGlobalLock(boolean globalLock) {
//            this.globalLock = globalLock;
//        }
//
//        public String getMapName() {
//            return mapName;
//        }
//
//        public void setMapName(String mapName) {
//            this.mapName = mapName;
//        }
//
//        public Vertex getFrom() {
//            return from;
//        }
//
//        public Vertex getTo() {
//            return to;
//        }
//
//        public Object getKey() {
//            return key;
//        }
//
//        public void setFrom(Vertex from) {
//            this.from = from;
//        }
//
//        public void setTo(Vertex to) {
//            this.to = to;
//        }
//
//        public void setKey(Object key) {
//            this.key = key;
//        }
//    }
//
//    public static class Vertex implements java.io.Serializable {
//        int NOT_VISITED = 0;
//        int BEING_VISITED = 1;
//        int DONE_VISITED = 2;
//        int visited = NOT_VISITED;
//        DistributedLock owner;
//        List<Edge> incomings = new ArrayList<Edge>();
//        List<Edge> outgoings = new ArrayList<Edge>();
//
//        Vertex(DistributedLock owner) {
//            this.owner = owner;
//        }
//
//        public void addIncoming(Edge edge) {
//            incomings.add(edge);
//        }
//
//        public void addOutgoing(Edge edge) {
//            outgoings.add(edge);
//        }
//
//        public Vertex visit(List<Edge> list) {
//            if (visited == BEING_VISITED) {
//                return this;
//            }
//            if (visited == DONE_VISITED) {
//                return null;
//            }
//            visited = BEING_VISITED;
//            for (Edge edge : outgoings) {
//                Vertex v2 = edge.to.visit(list);
//                if (v2 != null) {
//                    list.add(edge);
//                    if (v2 == this) {
//                        throw new RuntimeException("Cycle is detected!");
//                    }
//                    return v2;
//                }
//            }
//            visited = DONE_VISITED;
//            return null;
//        }
//
//        public int getVisited() {
//            return visited;
//        }
//
//        public DistributedLock getOwner() {
//            return owner;
//        }
//
//        public List<Edge> getIncomings() {
//            return incomings;
//        }
//
//        public List<Edge> getOutgoings() {
//            return outgoings;
//        }
//
//        public String name() {
//            return owner.getLockAddress() + ":" + owner.getLockThreadId();
//        }
//
//        @Override
//        public boolean equals(Object o) {
//            if (this == o) return true;
//            if (o == null || getClass() != o.getClass()) return false;
//            Vertex vertex = (Vertex) o;
//            if (owner != null ? !(owner.getLockAddress().equals(vertex.owner.getLockAddress()) &&
//                    owner.getLockThreadId() == vertex.owner.getLockThreadId()) : vertex.owner != null) return false;
//            return true;
//        }
//
//        @Override
//        public int hashCode() {
//            return owner != null ? owner.hashCode() : 0;
//        }
//
//        @Override
//        public String toString() {
//            return "Vertex{" +
//                    "outgoings=" + outgoings +
//                    "," + owner +
//                    ", visited=" + visited +
//                    '}';
//        }
//    }
//}
