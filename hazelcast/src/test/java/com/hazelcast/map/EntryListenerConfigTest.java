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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.EventListener;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertTrue;

/**
 * Keep this test serial. It relays on static shared variables.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class EntryListenerConfigTest extends HazelcastTestSupport {

    private String mapName = randomMapName();
    private EntryListenerConfig listenerConfig = new EntryListenerConfig();
    private HazelcastInstance instance;

    @Before
    public void setUp() throws Exception {
        init();
    }

    @After
    public void tearDown() throws Exception {
        init();
    }

    @Test
    public void testMapListenerAddition_withClassName() throws Exception {
        listenerConfig.setClassName(TestMapListener.class.getCanonicalName());
        createInstanceAndInitializeListeners();

        assertListenerRegisteration();
    }

    @Test
    public void testMapListenerAddition_withImplementation() throws Exception {
        listenerConfig.setImplementation(new TestMapListener());
        createInstanceAndInitializeListeners();

        assertListenerRegisteration();
    }

    @Test
    public void testHazelcastInstanceAwareness_whenMapListenerAdded_withImplementation() throws Exception {
        listenerConfig.setImplementation(new TestMapListener());
        createInstanceAndInitializeListeners();

        assertInstanceSet(TestMapListener.INSTANCE_AWARE);
    }

    @Test
    public void testHazelcastInstanceAwareness_whenMapListenerAdded_withClassName() throws Exception {
        listenerConfig.setClassName(TestMapListener.class.getCanonicalName());
        createInstanceAndInitializeListeners();

        assertInstanceSet(TestMapListener.INSTANCE_AWARE);
    }

    @Test
    public void testEntryListenerAddition_withClassName() throws Exception {
        listenerConfig.setClassName(TestEntryListener.class.getCanonicalName());
        createInstanceAndInitializeListeners();

        assertListenerRegisteration();
    }

    @Test
    public void testEntryListenerAddition_withImplementation() throws Exception {
        listenerConfig.setImplementation(new TestEntryListener());
        createInstanceAndInitializeListeners();

        assertListenerRegisteration();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testListenerAddition_throwsException_withInvalidListener() throws Exception {
        listenerConfig.setImplementation(new EventListener() {
        });
    }

    @Test
    public void testHazelcastInstanceAwareness_whenEntryListenerAdded_withImplementation() throws Exception {
        listenerConfig.setImplementation(new TestEntryListener());
        createInstanceAndInitializeListeners();

        assertInstanceSet(TestEntryListener.INSTANCE_AWARE);
    }

    @Test
    public void testHazelcastInstanceAwareness_whenEntryListenerAdded_withClassName() throws Exception {
        listenerConfig.setClassName(TestEntryListener.class.getCanonicalName());
        createInstanceAndInitializeListeners();

        assertInstanceSet(TestEntryListener.INSTANCE_AWARE);
    }

    private void createInstanceAndInitializeListeners() {
        MapConfig mapConfig = new MapConfig(mapName);
        mapConfig.getEntryListenerConfigs().add(listenerConfig);
        Config config = new Config().addMapConfig(mapConfig);

        instance = createHazelcastInstance(config);
        instance.getMap(mapName);
    }

    private void assertListenerRegisteration() {
        boolean hasEventRegistration = getEventService().hasEventRegistration(SERVICE_NAME, mapName);
        assertTrue("Listener should be registered", hasEventRegistration);
    }

    private void assertInstanceSet(final AtomicBoolean instanceSet) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(instanceSet.get());
            }
        });
    }

    private EventService getEventService() {
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(instance);
        return nodeEngineImpl.getEventService();
    }

    private void init() {
        TestMapListener.INSTANCE_AWARE.set(false);
        TestEntryListener.INSTANCE_AWARE.set(false);
    }
}
