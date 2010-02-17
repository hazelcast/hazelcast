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

import static com.hazelcast.nio.IOUtil.toData;
import static com.hazelcast.nio.IOUtil.toObject;
import static junit.framework.Assert.assertEquals;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class CMapTest {
    @Test
    public void testPut() {
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
        assertEquals(1, cmap.ownedRecords.size());
        Record record = cmap.getRecord(dKey);
        assertNotNull(record);
        assertTrue(record.isActive());
        assertTrue(record.isValid());
    }

    private Request newPutRequest(Data key, Data value) {
        return newPutRequest(key, value, -1);
    }

    public static Request newPutRequest(Data key, Data value, long ttl) {
        Request request = new Request();
        request.setLocal(ClusterOperation.CONCURRENT_MAP_PUT, null, key, value, -1, -1, ttl, null);
        return request;
    }

    public static Request newGetRequest(Data key) {
        Request request = new Request();
        request.setLocal(ClusterOperation.CONCURRENT_MAP_GET, null, key, null, -1, -1, -1, null);
        return request;
    }
}
