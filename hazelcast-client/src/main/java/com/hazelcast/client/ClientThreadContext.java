///*
//* Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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
//package com.hazelcast.client;
//
//import java.util.Iterator;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.ConcurrentMap;
//
//public final class ClientThreadContext {
//
//    private static final ConcurrentMap<Thread, ClientThreadContext> mapContexts
//            = new ConcurrentHashMap<Thread, ClientThreadContext>(100);
//
//    public static ClientThreadContext get() {
//        Thread currentThread = Thread.currentThread();
//        ClientThreadContext threadContext = mapContexts.get(currentThread);
//        if (threadContext == null) {
//            threadContext = new ClientThreadContext(currentThread);
//            mapContexts.put(currentThread, threadContext);
//            Iterator<Thread> threads = mapContexts.keySet().iterator();
//            while (threads.hasNext()) {
//                Thread thread = threads.next();
//                if (!thread.isAlive()) {
//                    threads.remove();
//                }
//            }
//        }
//        return threadContext;
//    }
//
//    public static void shutdown() {
//        mapContexts.clear();
//    }
//
//    private final ClientSerializer serializer = new ClientSerializer();
//
//    private final Thread thread;
//
//    public ClientThreadContext(Thread thread) {
//        this.thread = thread;
//    }
//
//    public byte[] toByte(Object object) {
//        return serializer.toByteArray(object);
//    }
//
//    public Object toObject(byte[] bytes) {
//        return serializer.toObject(bytes);
//    }
//}
