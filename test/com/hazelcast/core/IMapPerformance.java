/*
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.core;

import org.junit.Test;

public class IMapPerformance {

    @Test
    public void testMapPutWithSameKey(){
        IMap<String,String> map = Hazelcast.getMap("testMapPutWithSameKey");
        long ops = 100000;
        Timer t = new Timer(ops);
        for(int i=0; i<ops; ++i){
            map.put("Hello","World");
        }
        t.stop();
        t.printResult("testMapPutWithSameKey");
        map.clear();
    }

    @Test
    public void testMapPutWithDifferentKey(){
        IMap<Integer,String> map = Hazelcast.getMap("testMapPutWithDifferentKey");
        long ops = 100000;
        Timer t = new Timer(ops);
        for(int i=0; i<ops; ++i){
            map.put(i,"Hello World");
        }
        t.stop();
        t.printResult("testMapPutWithDifferentKey");
        map.clear();
    }

    @Test
    public void testMapRemove(){
        IMap<Integer,String> map = Hazelcast.getMap("testMapRemove");
        long ops = 100000;
        for(int i=0; i<ops; ++i){
            map.put(i,"Hello World");
        }
        
        Timer t = new Timer(ops/10);
        for(int i=0; i<ops/10; ++i){
            int index = (int)((int)ops*Math.random());
            map.remove(index);
        }
        t.stop();
        t.printResult("testMapRemove");
        map.clear();
    }

    @Test
    public void testMapValues(){
        IMap<Integer,String> map = Hazelcast.getMap("testMapValues");
        long ops = 100000;
        for(int i=0; i<ops; ++i){
            map.put(i,"Hello World");
        }

        Timer t = new Timer(ops);
        for(String value: map.values()){
            String a = value;
        }
        t.stop();
        t.printResult("testMapValues");
        map.clear();
    }

    @Test
    public void testMapReplace(){
        IMap<Integer,String> map = Hazelcast.getMap("testMapReplace");
        long ops = 100000;
        for(int i=0; i<ops; ++i){
            map.put(i,"Hello World");
        }

        Timer t = new Timer(ops);
        for(int i=0; i<ops; ++i){
            map.replace(i,"FooBar");
        }
        t.stop();
        t.printResult("testMapReplace");
        map.clear();
    }

    @Test
    public void testMapContainsKey(){
        String test = "testMapContainsKey";
        IMap<Integer,String> map = Hazelcast.getMap(test);
        long ops = 100000;
        for(int i=0; i<ops; ++i){
            map.put(i,"Hello World");
        }

        Timer t = new Timer(ops);
        for(int i=0; i<ops; ++i){
            map.containsKey(i);
        }
        t.stop();
        t.printResult(test);
        map.clear();
    }

    @Test
    public void testMapContainsValue(){
        String test = "testMapContainsValue";
        IMap<Integer,String> map = Hazelcast.getMap(test);
        long ops = 100000;
        for(int i=0; i<ops; ++i){
            map.put(i,"Hello World" +i);
        }

        Timer t = new Timer(ops);
        for(int i=0; i<ops; ++i){
            map.containsValue("Hello World" +i);
        }
        t.stop();
        t.printResult(test);
        map.clear();
    }

}
