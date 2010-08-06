/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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

package com.hazelcast.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.nio.Data;
import org.junit.Test;

import static com.hazelcast.impl.Constants.Objects.OBJECT_REDO;
import static com.hazelcast.nio.IOUtil.toData;
import static com.hazelcast.nio.IOUtil.toObject;
import static junit.framework.Assert.assertEquals;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class CMapTest {

    @Test
    public void testTTL() throws Exception {
        Config config = new XmlConfigBuilder().build();
        FactoryImpl mockFactory = mock(FactoryImpl.class);
        Node node = new Node(mockFactory, config);
        node.serviceThread = Thread.currentThread();
        CMap cmap = new CMap(node.concurrentMapManager, "c:myMap");
        Object key = "1";
        Object value = "istanbul";
        Data dKey = toData(key);
        Data dValue = toData(value);
        Request reqPut = newPutRequest(dKey, dValue);
        reqPut.ttl = 3000;
        cmap.put(reqPut);
        assertTrue(cmap.mapRecords.containsKey(toData(key)));
        Data actualValue = cmap.get(newGetRequest(dKey));
        assertThat(toObject(actualValue), equalTo(value));
        assertEquals(1, cmap.mapRecords.size());
        Record record = cmap.getRecord(dKey);
        assertNotNull(record);
        assertTrue(record.isActive());
        assertTrue(record.isValid());
        assertEquals(1, cmap.size());
        assertNotNull(cmap.locallyOwnedMap);
        assertNotNull(cmap.get(newGetRequest(dKey)));
        assertEquals(dValue, cmap.get(newGetRequest(dKey)));
        assertEquals(value, cmap.locallyOwnedMap.get(key));
        assertEquals(1, cmap.locallyOwnedMap.mapCache.size());
        Thread.sleep(5000);
        cmap.locallyOwnedMap.evict(System.currentTimeMillis());
        assertEquals(0, cmap.locallyOwnedMap.mapCache.size());
        assertEquals(OBJECT_REDO, cmap.locallyOwnedMap.get(key));
        assertEquals(0, cmap.size());
        assertTrue(cmap.evict(newEvictRequest(dKey)));
        assertTrue(cmap.shouldPurgeRecord(record, System.currentTimeMillis() + 10000));
        cmap.removeAndPurgeRecord(record);
        assertEquals(0, cmap.mapRecords.size());
    }

    @Test
    public void testPut() throws Exception {
        Config config = new XmlConfigBuilder().build();
        FactoryImpl mockFactory = mock(FactoryImpl.class);
        Node node = new Node(mockFactory, config);
        node.serviceThread = Thread.currentThread();
        CMap cmap = new CMap(node.concurrentMapManager, "c:myMap");
        Object key = "1";
        Object value = "istanbul";
        Data dKey = toData(key);
        Data dValue = toData(value);
        cmap.put(newPutRequest(dKey, dValue));
        assertTrue(cmap.mapRecords.containsKey(toData(key)));
        Data actualValue = cmap.get(newGetRequest(dKey));
        assertThat(toObject(actualValue), equalTo(value));
        assertEquals(1, cmap.mapRecords.size());
        Record record = cmap.getRecord(dKey);
        assertNotNull(record);
        assertTrue(record.isActive());
        assertTrue(record.isValid());
        assertEquals(1, cmap.size());
        cmap.remove(newRemoveRequest(dKey));
        assertTrue(System.currentTimeMillis() - record.getRemoveTime() < 100);
        assertEquals(1, cmap.mapRecords.size());
        record = cmap.getRecord(dKey);
        assertNotNull(record);
        assertFalse(record.isActive());
        assertTrue(record.isValid());
        assertEquals(0, cmap.size());
        cmap.put(newPutRequest(dKey, dValue, 1000));
        assertEquals(0, record.getRemoveTime());
        assertTrue(cmap.mapRecords.containsKey(toData(key)));
        Thread.sleep(1000);
        assertEquals(0, cmap.size());
        assertFalse(cmap.contains(newContainsRequest(dKey, null)));
    }

    public static Request newPutRequest(Data key, Data value) {
        return newPutRequest(key, value, -1);
    }

    public static Request newPutRequest(Data key, Data value, long ttl) {
        return newRequest(ClusterOperation.CONCURRENT_MAP_PUT, key, value, ttl);
    }

    public static Request newRequest(ClusterOperation operation, Data key, Data value, long ttl) {
        Request request = new Request();
        request.setLocal(operation, null, key, value, -1, -1, ttl, null);
        return request;
    }

    public static Request newRemoveRequest(Data key) {
        return newRequest(ClusterOperation.CONCURRENT_MAP_REMOVE, key, null, -1);
    }

    public static Request newEvictRequest(Data key) {
        return newRequest(ClusterOperation.CONCURRENT_MAP_EVICT, key, null, -1);
    }

    public static Request newGetRequest(Data key) {
        return newRequest(ClusterOperation.CONCURRENT_MAP_GET, key, null, -1);
    }

    public static Request newContainsRequest(Data key, Data value) {
        return newRequest(ClusterOperation.CONCURRENT_MAP_CONTAINS, key, value, -1);
    }
}
