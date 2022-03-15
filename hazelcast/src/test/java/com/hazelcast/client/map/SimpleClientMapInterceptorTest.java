/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
 */

package com.hazelcast.client.map;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.helpers.PortableHelpersFactory;
import com.hazelcast.client.helpers.SimpleClientInterceptor;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.InterceptorTest;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.UuidUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SimpleClientMapInterceptorTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    private HazelcastInstance client;
    private HazelcastInstance server1;
    private HazelcastInstance server2;

    private SimpleClientInterceptor interceptor;

    @Before
    public void setup() {
        Config config = getConfig();
        config.getSerializationConfig().addPortableFactory(PortableHelpersFactory.ID, new PortableHelpersFactory());
        server1 = hazelcastFactory.newHazelcastInstance(config);
        server2 = hazelcastFactory.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getSerializationConfig().addPortableFactory(PortableHelpersFactory.ID, new PortableHelpersFactory());
        client = hazelcastFactory.newHazelcastClient(clientConfig);

        interceptor = new SimpleClientInterceptor();
    }

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void clientMapInterceptorTestIssue1238() throws InterruptedException {
        final IMap<Object, Object> map = client.getMap("clientMapInterceptorTest");

        String id = map.addInterceptor(interceptor);

        map.put(1, "New York");
        map.put(2, "Istanbul");
        map.put(3, "Tokyo");
        map.put(4, "London");
        map.put(5, "Paris");
        map.put(6, "Cairo");
        map.put(7, "Hong Kong");

        map.remove(1);

        try {
            map.remove(2);
            fail();
        } catch (Exception ignore) {

        }

        assertEquals(map.size(), 6);
        assertEquals(map.get(1), null);
        assertEquals(map.get(2), "ISTANBUL:");
        assertEquals(map.get(3), "TOKYO:");
        assertEquals(map.get(4), "LONDON:");
        assertEquals(map.get(5), "PARIS:");
        assertEquals(map.get(6), "CAIRO:");
        assertEquals(map.get(7), "HONG KONG:");

        map.removeInterceptor(id);
        map.put(8, "Moscow");

        assertEquals(map.get(8), "Moscow");
        assertEquals(map.get(1), null);
        assertEquals(map.get(2), "ISTANBUL");
        assertEquals(map.get(3), "TOKYO");
        assertEquals(map.get(4), "LONDON");
        assertEquals(map.get(5), "PARIS");
        assertEquals(map.get(6), "CAIRO");
        assertEquals(map.get(7), "HONG KONG");
    }

    @Test
    public void removeInterceptor_returns_true_when_interceptor_removed() {
        String mapName = "mapWithInterceptor";
        IMap map = client.getMap(mapName);
        String id = map.addInterceptor(new InterceptorTest.SimpleInterceptor());

        assertTrue(map.removeInterceptor(id));
        assertNoRegisteredInterceptorExists(server1.getMap(mapName));
        assertNoRegisteredInterceptorExists(server2.getMap(mapName));
    }

    @Test
    public void removeInterceptor_returns_false_when_there_is_no_interceptor() {
        String mapName = "mapWithNoInterceptor";
        IMap map = client.getMap(mapName);

        assertFalse(map.removeInterceptor(UuidUtil.newUnsecureUuidString()));
        assertNoRegisteredInterceptorExists(server1.getMap(mapName));
        assertNoRegisteredInterceptorExists(server2.getMap(mapName));
    }

    private static void assertNoRegisteredInterceptorExists(IMap map) {
        String mapName = map.getName();
        MapService mapservice = (MapService) (((MapProxyImpl) map).getService());
        mapservice.getMapServiceContext().getMapContainer(mapName).getInterceptorRegistry().getInterceptors();
    }

}
