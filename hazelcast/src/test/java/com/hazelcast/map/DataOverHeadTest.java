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
//package com.hazelcast.map.test;
//
//import com.hazelcast.nio.serialization.Data;
//import com.hazelcast.nio.FastDataOutputStream;
//import com.hazelcast.nio.serialization.DefaultSerializers;
//import org.junit.Test;
//
//import java.io.DataOutput;
//import java.io.IOException;
//import java.util.Map;
//import java.util.concurrent.ConcurrentHashMap;
//
//public class DataOverHeadTest {
//    int recordCount = 10000;
//
//    @Test
//    public void testOverheadWithByteArray() throws Exception {
//        Map map = new ConcurrentHashMap();
//        int bytecount = 200;
//        System.gc();
//        long usedBefore = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
//        for (int i = 0; i < recordCount; i++) {
//            map.put(i, new byte[bytecount]);
//        }
//        System.gc();
//        long usedAfter = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
//        long used = usedAfter - usedBefore;
//        System.out.println("1-Used Memory:" + (used / 1024 ) + " KB");
//        long mem1 = used / recordCount;
//        System.out.println("1-Memory Per Record:" + mem1 + " bytes");
//
//        map.clear();
//
//
//        FastDataOutputStream out = new FastDataOutputStream();
//        System.gc();
//        usedBefore = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
//        DefaultSerializers.ByteArraySerializer bs = new DefaultSerializers.ByteArraySerializer();
//        for (int i = 0; i < recordCount; i++) {
//            Data data = new Data();
//            bs.write(out, new byte[bytecount]);
//            data.writeData(out);
//            map.put(i, data);
//        }
//        System.gc();
//        usedAfter = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
//        used = usedAfter - usedBefore;
//        long mem2 = used / recordCount;
//        System.out.println("2-Used Memory:" + (used / 1024) + " KB");
//        System.out.println("2-Memory Per Record:" + mem2 + " bytes");
//        System.out.println("Overhead of data (byte array):" + (mem2 - mem1));
//        System.out.println();
//
//    }
//
//    @Test
//    public void testOverheadString() throws Exception {
//        Map map = new ConcurrentHashMap();
//        String testString = "Hello World! 1234567???";
//        System.gc();
//        long usedBefore = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
//        for (int i = 0; i < recordCount; i++) {
//            map.put(i, new String(testString+i));
//        }
//        System.gc();
//        long usedAfter = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
//        long used = usedAfter - usedBefore;
//        System.out.println("1-Used Memory:" + (used / 1024 ) + " KB");
//        long mem1 = used / recordCount;
//        System.out.println("1-Memory Per Record:" + mem1 + " bytes");
//
//        map.clear();
//
//
//        FastDataOutputStream out = new FastDataOutputStream();
//        System.gc();
//        usedBefore = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
//        DefaultSerializers.StringSerializer bs = new DefaultSerializers.StringSerializer();
//        for (int i = 0; i < recordCount; i++) {
//            Data data = new Data();
//            bs.write(out, new String(testString+i));
//            data.writeData(out);
//            map.put(i, data);
//        }
//        System.gc();
//        usedAfter = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
//        used = usedAfter - usedBefore;
//        long mem2 = used / recordCount;
//        System.out.println("2-Used Memory:" + (used / 1024) + " KB");
//        System.out.println("2-Memory Per Record:" + mem2 + " bytes");
//        System.out.println("Overhead of data (String):" + (mem2 - mem1));
//        System.out.println();
//
//    }
//
//    @Test
//    public void testOverheadInteger() throws Exception {
//        Map map = new ConcurrentHashMap();
//        System.gc();
//        long usedBefore = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
//        for (int i = 0; i < recordCount; i++) {
//            map.put(i, i);
//        }
//        System.gc();
//        long usedAfter = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
//        long used = usedAfter - usedBefore;
//        System.out.println("1-Used Memory:" + (used / 1024 ) + " KB");
//        long mem1 = used / recordCount;
//        System.out.println("1-Memory Per Record:" + mem1 + " bytes");
//
//        map.clear();
//
//
//        FastDataOutputStream out = new FastDataOutputStream();
//        System.gc();
//        usedBefore = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
//        DefaultSerializers.IntegerSerializer bs = new DefaultSerializers.IntegerSerializer();
//        for (int i = 0; i < recordCount; i++) {
//            Data data = new Data();
//            bs.write(out, i);
//            data.writeData(out);
//            map.put(i, data);
//        }
//        System.gc();
//        usedAfter = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
//        used = usedAfter - usedBefore;
//        long mem2 = used / recordCount;
//        System.out.println("2-Used Memory:" + (used / 1024) + " KB");
//        System.out.println("2-Memory Per Record:" + mem2 + " bytes");
//        System.out.println("Overhead of data (int):" + (mem2 - mem1));
//        System.out.println();
//
//    }
//
//}
