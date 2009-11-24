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
package com.hazelcast.client;

import org.junit.After;
import org.junit.Test;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MultiMap;
import static com.hazelcast.client.TestUtility.getHazelcastClient;

import java.util.Map;
import java.util.HashMap;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static junit.framework.Assert.assertTrue;

public class HazelcastClientMultiMapTest {

     private HazelcastClient hClient;



    @After
    public void shutdownAll() throws InterruptedException{
    	Hazelcast.shutdownAll();
    	if(hClient!=null){	hClient.shutdown(); }
    	Thread.sleep(500);
    }

    @Test
    public void putToMultiMap(){
        HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
        hClient = getHazelcastClient(h);
        MultiMap multiMap = hClient.getMultiMap("default");
        assertTrue(multiMap.put("a",1));
    }
    @Test
    public void removeFromMultiMap(){
        HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
        hClient = getHazelcastClient(h);
        MultiMap multiMap = hClient.getMultiMap("default");
        assertTrue(multiMap.put("a",1));
        assertTrue(multiMap.remove("a",1));
    }

    @Test
    public void containsKey(){
        HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
        hClient = getHazelcastClient(h);
        MultiMap multiMap = hClient.getMultiMap("default");
        assertFalse(multiMap.containsKey("a"));
        assertTrue(multiMap.put("a",1));
        assertTrue(multiMap.containsKey("a"));

    }
    @Test
    public void containsValue(){
        HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
        hClient = getHazelcastClient(h);
        MultiMap multiMap = hClient.getMultiMap("default");
        assertFalse(multiMap.containsValue(1));
        assertTrue(multiMap.put("a",1));
        assertTrue(multiMap.containsValue(1));
    }
    @Test
    public void containsEntry(){
        HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
        hClient = getHazelcastClient(h);
        MultiMap multiMap = hClient.getMultiMap("default");
        assertFalse(multiMap.containsEntry("a",1));
        assertTrue(multiMap.put("a",1));
        assertTrue(multiMap.containsEntry("a",1));
        assertFalse(multiMap.containsEntry("a",2));
    }
    @Test
    public void size(){
        HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
        hClient = getHazelcastClient(h);
        MultiMap multiMap = hClient.getMultiMap("default");
        assertEquals(0, multiMap.size());
        assertTrue(multiMap.put("a",1));
        assertEquals(1, multiMap.size());

        assertTrue(multiMap.put("a",2));
        assertEquals(2, multiMap.size());
    }

    @Test
    public void get() throws InterruptedException {
        HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
        hClient = getHazelcastClient(h);
        MultiMap multiMap = hClient.getMultiMap("default");
        assertTrue(multiMap.put("a",1));
        assertTrue(multiMap.put("a",2));
        Map<Integer, CountDownLatch> map = new HashMap<Integer, CountDownLatch>();
        map.put(1, new CountDownLatch(1));
        map.put(2, new CountDownLatch(1));
        Collection collection = multiMap.get("a");
        assertEquals(2, collection.size());
        for(Iterator it = collection.iterator();it.hasNext();){
            Object o = it.next();
            map.get((Integer)o).countDown();
        }
        assertTrue(map.get(1).await(10, TimeUnit.MILLISECONDS));
        assertTrue(map.get(2).await(10, TimeUnit.MILLISECONDS));
    }
}
