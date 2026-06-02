/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

import classloading.NonHzTestEntryListener;
import com.hazelcast.config.Config;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.FilteringClassLoader;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.LogEntryMatcher;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.logging.log4j.core.LogEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.EventListener;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static java.lang.Thread.currentThread;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertTrue;

/**
 * Keep this test serial. It relays on static shared variables.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class EntryListenerConfigTest extends HazelcastTestSupport {

    private final String mapName = randomMapName();
    private final EntryListenerConfig listenerConfig = new EntryListenerConfig();
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
    public void testMapListenerAddition_withClassName() {
        listenerConfig.setClassName(TestMapListener.class.getCanonicalName());
        createInstanceAndInitializeListeners();

        assertListenerRegistration();
    }

    @Test
    public void testMapListenerAddition_withImplementation() {
        listenerConfig.setImplementation(new TestMapListener());
        createInstanceAndInitializeListeners();

        assertListenerRegistration();
    }

    @Test
    public void testHazelcastInstanceAwareness_whenMapListenerAdded_withImplementation() {
        listenerConfig.setImplementation(new TestMapListener());
        createInstanceAndInitializeListeners();

        assertInstanceSet(TestMapListener.INSTANCE_AWARE);
    }

    @Test
    public void testHazelcastInstanceAwareness_whenMapListenerAdded_withClassName() {
        listenerConfig.setClassName(TestMapListener.class.getCanonicalName());
        createInstanceAndInitializeListeners();

        assertInstanceSet(TestMapListener.INSTANCE_AWARE);
    }

    @Test
    public void testEntryListenerAddition_withClassName() {
        listenerConfig.setClassName(TestEntryListener.class.getCanonicalName());
        createInstanceAndInitializeListeners();

        assertListenerRegistration();
    }

    @Test
    public void testEntryListenerAddition_withImplementation() {
        listenerConfig.setImplementation(new TestEntryListener());
        createInstanceAndInitializeListeners();

        assertListenerRegistration();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testListenerAddition_throwsException_withInvalidListener() {
        listenerConfig.setImplementation(new EventListener() {
        });
    }

    @Test
    public void testHazelcastInstanceAwareness_whenEntryListenerAdded_withImplementation() {
        listenerConfig.setImplementation(new TestEntryListener());
        createInstanceAndInitializeListeners();

        assertInstanceSet(TestEntryListener.INSTANCE_AWARE);
    }


    /**
     * Scenario:
     * <ol>
     *     <li>Member started in isolated classloading environment</li>
     *     <li>Member adds dynamic config (it's still done via operations)</li>
     *     <li>Second node fails due to lack of given class.</li>
     * </ol>
     */
    @Test
    public void testHazelcastInstanceAwareness_whenEntryListenerAdded_withImplementationMissing() {
        TestHazelcastInstanceFactory instanceFactory = createHazelcastInstanceFactory(1);

        FilteringClassLoader filteringClassLoader = new FilteringClassLoader(List.of("classloading"), null);

        Config blankConfig = new Config();
        blankConfig.setClassLoader(filteringClassLoader);
        instance = instanceFactory.newHazelcastInstance(blankConfig);

        ClassLoader before = Thread.currentThread().getContextClassLoader();
        try {
            currentThread().setContextClassLoader(filteringClassLoader);

            listenerConfig.setImplementation(new NonHzTestEntryListener());
            MapConfig mapConfig = new MapConfig(mapName);
            mapConfig.getEntryListenerConfigs().add(listenerConfig);

            assertThatThrownBy(() -> instance.getConfig().addMapConfig(mapConfig))
                .isInstanceOf(HazelcastSerializationException.class)
                .hasMessageContaining("ListenerConfig's implementation");
        } finally {
            currentThread().setContextClassLoader(before);
        }
    }

    /**
     * Scenario:
     * <ol>
     *     <li>Member started</li>
     *     <li>Member adds dynamic config</li>
     *     <li>Second member starts in isolated classloading environment</li>
     *     <li>startup Ops cause the exchange of dynamic config</li>
     *     <li>Second node fails due to lack of given class.</li>
     * </ol>
     */
    @Test
    public void testHazelcastInstanceAwareness_whenEntryListenerAndScaling_withImplementationMissing() {
        TestHazelcastInstanceFactory instanceFactory = createHazelcastInstanceFactory(2);
        FilteringClassLoader filteringClassLoader = new FilteringClassLoader(List.of("classloading"), null);

        var config = new Config();
        config.setClassLoader(filteringClassLoader);

        listenerConfig.setImplementation(new NonHzTestEntryListener());
        MapConfig mapConfig = new MapConfig(mapName);
        mapConfig.getEntryListenerConfigs().add(listenerConfig);

        instance = instanceFactory.newHazelcastInstance(config);
        instance.getConfig().addMapConfig(mapConfig);

        ClassLoader before = Thread.currentThread().getContextClassLoader();
        Predicate<LogEvent> logEventPredicate = event ->
            event.getThrown() != null
                && event.getThrown().getMessage().contains("Unable to deserialize EntryListenerConfig's implementation");
        try (LogEntryMatcher matcher = LogEntryMatcher.install(logEventPredicate)) {
            currentThread().setContextClassLoader(filteringClassLoader);

            assertThatThrownBy(() -> instanceFactory.newHazelcastInstance(config))
                            .isInstanceOf(IllegalStateException.class);
            assertThat(matcher.matched()).isTrue();
        } finally {
            currentThread().setContextClassLoader(before);
        }
    }

    @Test
    public void testHazelcastInstanceAwareness_whenEntryListenerAdded_withClassName() {
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

    private void assertListenerRegistration() {
        boolean hasEventRegistration = getEventService().hasEventRegistration(SERVICE_NAME, mapName);
        assertTrue("Listener should be registered", hasEventRegistration);
    }

    private void assertInstanceSet(final AtomicBoolean instanceSet) {
        assertTrueEventually(() -> assertTrue(instanceSet.get()));
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
