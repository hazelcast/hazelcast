/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.tcp.iobalancer;

import com.hazelcast.nio.tcp.IOSelector;
import com.hazelcast.nio.tcp.MigratableHandler;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.ItemCounter;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.test.TestCollectionUtils.setOf;
import static java.util.Collections.singleton;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class EventCountBasicMigrationStrategyTest extends HazelcastTestSupport {

    private Map<IOSelector, Set<MigratableHandler>> selectorToHandlers;
    private ItemCounter<MigratableHandler> handlerEventsCounter;
    private LoadImbalance imbalance;

    private EventCountBasicMigrationStrategy strategy;

    @Before
    public void setUp() {
        selectorToHandlers = new HashMap<IOSelector, Set<MigratableHandler>>();
        handlerEventsCounter = new ItemCounter<MigratableHandler>();
        imbalance = new LoadImbalance(selectorToHandlers, handlerEventsCounter);
        strategy = new EventCountBasicMigrationStrategy();
    }

    @Test
    public void testImbalanceDetected_shouldReturnFalseWhenNoKnownMinimum() throws Exception {
        imbalance.minimumEvents = Long.MIN_VALUE;

        boolean imbalanceDetected = strategy.imbalanceDetected(imbalance);
        assertFalse(imbalanceDetected);
    }

    @Test
    public void testImbalanceDetected_shouldReturnFalseWhenNoKnownMaximum() throws Exception {
        imbalance.maximumEvents = Long.MAX_VALUE;

        boolean imbalanceDetected = strategy.imbalanceDetected(imbalance);
        assertFalse(imbalanceDetected);
    }

    @Test
    public void testImbalanceDetected_shouldReturnFalseWhenBalanced() throws Exception {
        imbalance.maximumEvents = 1000;
        imbalance.minimumEvents = (long) (1000 * 0.8);

        boolean imbalanceDetected = strategy.imbalanceDetected(imbalance);
        assertFalse(imbalanceDetected);
    }

    @Test
    public void testImbalanceDetected_shouldReturnTrueWhenNotBalanced() throws Exception {
        imbalance.maximumEvents = 1000;
        imbalance.minimumEvents = (long) (1000 * 0.8) - 1;

        boolean imbalanceDetected = strategy.imbalanceDetected(imbalance);
        assertTrue(imbalanceDetected);
    }


    @Test
    public void testFindHandlerToMigrate() throws Exception {
        IOSelector sourceSelector = mock(IOSelector.class);
        IOSelector destinationSelector = mock(IOSelector.class);
        imbalance.sourceSelector = sourceSelector;
        imbalance.destinationSelector = destinationSelector;

        imbalance.minimumEvents = 100;
        MigratableHandler handler1 = mock(MigratableHandler.class);
        handlerEventsCounter.set(handler1, 100l);
        selectorToHandlers.put(destinationSelector, singleton(handler1));

        imbalance.maximumEvents = 300;
        MigratableHandler handler2 = mock(MigratableHandler.class);
        MigratableHandler handler3 = mock(MigratableHandler.class);
        handlerEventsCounter.set(handler2, 200l);
        handlerEventsCounter.set(handler3, 100l);
        selectorToHandlers.put(sourceSelector, setOf(handler2, handler3));

        MigratableHandler handlerToMigrate = strategy.findHandlerToMigrate(imbalance);
        assertEquals(handler3, handlerToMigrate);
    }

}