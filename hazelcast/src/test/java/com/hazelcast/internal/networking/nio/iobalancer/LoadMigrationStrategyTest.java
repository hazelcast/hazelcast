/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.networking.nio.iobalancer;

import com.hazelcast.internal.networking.nio.MigratableHandler;
import com.hazelcast.internal.networking.nio.NioThread;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class LoadMigrationStrategyTest extends HazelcastTestSupport {

    private Map<NioThread, Set<MigratableHandler>> selectorToHandlers;
    private ItemCounter<MigratableHandler> handlerEventsCounter;
    private LoadImbalance imbalance;

    private LoadMigrationStrategy strategy;

    @Before
    public void setUp() {
        selectorToHandlers = new HashMap<NioThread, Set<MigratableHandler>>();
        handlerEventsCounter = new ItemCounter<MigratableHandler>();
        imbalance = new LoadImbalance(selectorToHandlers, handlerEventsCounter);
        strategy = new LoadMigrationStrategy();
    }

    @Test
    public void testImbalanceDetected_shouldReturnFalseWhenNoKnownMinimum() throws Exception {
        imbalance.minimumLoad = Long.MIN_VALUE;

        boolean imbalanceDetected = strategy.imbalanceDetected(imbalance);
        assertFalse(imbalanceDetected);
    }

    @Test
    public void testImbalanceDetected_shouldReturnFalseWhenNoKnownMaximum() throws Exception {
        imbalance.maximumLoad = Long.MAX_VALUE;

        boolean imbalanceDetected = strategy.imbalanceDetected(imbalance);
        assertFalse(imbalanceDetected);
    }

    @Test
    public void testImbalanceDetected_shouldReturnFalseWhenBalanced() throws Exception {
        imbalance.maximumLoad = 1000;
        imbalance.minimumLoad = (long) (1000 * 0.8);

        boolean imbalanceDetected = strategy.imbalanceDetected(imbalance);
        assertFalse(imbalanceDetected);
    }

    @Test
    public void testImbalanceDetected_shouldReturnTrueWhenNotBalanced() throws Exception {
        imbalance.maximumLoad = 1000;
        imbalance.minimumLoad = (long) (1000 * 0.8) - 1;

        boolean imbalanceDetected = strategy.imbalanceDetected(imbalance);
        assertTrue(imbalanceDetected);
    }

    @Test
    public void testFindHandlerToMigrate() throws Exception {
        NioThread sourceSelector = mock(NioThread.class);
        NioThread destinationSelector = mock(NioThread.class);
        imbalance.sourceSelector = sourceSelector;
        imbalance.destinationSelector = destinationSelector;

        imbalance.minimumLoad = 100;
        MigratableHandler handler1 = mock(MigratableHandler.class);
        handlerEventsCounter.set(handler1, 100L);
        selectorToHandlers.put(destinationSelector, singleton(handler1));

        imbalance.maximumLoad = 300;
        MigratableHandler handler2 = mock(MigratableHandler.class);
        MigratableHandler handler3 = mock(MigratableHandler.class);
        handlerEventsCounter.set(handler2, 200L);
        handlerEventsCounter.set(handler3, 100L);
        selectorToHandlers.put(sourceSelector, setOf(handler2, handler3));

        MigratableHandler handlerToMigrate = strategy.findHandlerToMigrate(imbalance);
        assertEquals(handler3, handlerToMigrate);
    }
}
