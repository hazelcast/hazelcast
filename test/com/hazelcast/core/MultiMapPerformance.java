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
import org.junit.After;
import static org.junit.Assert.assertEquals;

import java.util.Collection;

public class MultiMapPerformance extends PerformanceTest{
    private MultiMap<String,String> map = Hazelcast.getMultiMap("MultiMapPerformance");

    @After
    public void clear(){
        t.stop();
        t.printResult();
        map.clear();
        assertEquals(0,map.size());
    }

    @Test
    public void testMultiMapPutWithSameKeyAndValue(){
        String test = "testMultiMapPutWithSameKeyAndValue";
        t = new PerformanceTimer(test, ops);
        for(int i=0; i<ops; ++i){
            map.put("Hello","World");
        }
    }

    @Test
    public void testMultiMapPutWithSameKeyAndDifferentValue(){
        String test = "testMultiMapPutWithSameKeyAndDifferentValue";
        ops/=100;
        t = new PerformanceTimer(test, ops);
        for(int i=0; i<ops; ++i){
            map.put("Hello","World"+i);
        }
        ops*=100;
    }

    @Test
    public void testMultiMapPutWithDifferentKey(){
        String test = "testMultiMapPutWithDifferentKey";
        t = new PerformanceTimer(test, ops);
        for(int i=0; i<ops; ++i){
            map.put("Hello"+i,"World");
        }
    }

    @Test
    public void testMultiMapValues(){
        String test = "testMultiMapValues";
        for(int i=0; i<ops; ++i){
            map.put("Hello"+i,"World");
        }
        t = new PerformanceTimer(test, ops);
        Collection<String> values = map.values();
    }

    @Test
    public void testMultiMapGet(){
        String test = "testMultiMapGet";
        for(int i=0; i<ops; ++i){
            map.put("Hello"+i,"World");
        }
        t = new PerformanceTimer(test, ops);
        for(int i=0; i< ops; ++i){
            Collection<String> values = map.get("Hello"+i);
        }
    }

}
