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
import static org.junit.Assert.assertTrue;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class CMapTest {
    @Test
    public void testPut() {
        Config config = new XmlConfigBuilder().build();
        FactoryImpl mockFactory = mock(FactoryImpl.class);
        Node node = new Node(mockFactory, config);
        node.serviceThread = Thread.currentThread();
        Object key = "1";
        Object value = "istanbul";
        CMap cmap = new CMap(node.concurrentMapManager, "c:myMap");
        Request putRequest = new Request();
        putRequest.setLocal(ClusterOperation.CONCURRENT_MAP_PUT, null, toData(key), toData(value), -1, -1, -1, null);
        cmap.put(putRequest);
        assertTrue(cmap.mapRecords.containsKey(toData(key)));
        Request getRequest = new Request();
        getRequest.setLocal(ClusterOperation.CONCURRENT_MAP_GET, null, toData(key), null, -1, -1, -1, null);
        Data actualValue = cmap.get(getRequest);
        assertThat(toObject(actualValue), equalTo(value));
    }



}
