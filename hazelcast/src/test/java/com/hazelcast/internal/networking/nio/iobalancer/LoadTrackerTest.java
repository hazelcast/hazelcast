/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.logging.ILogger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class LoadTrackerTest {

    private NioThread selector1;
    private NioThread selector2;

    private NioThread[] selectors;
    private LoadTracker loadTracker;

    @Before
    public void setUp() {
        selector1 = mock(NioThread.class);
        selector2 = mock(NioThread.class);
        selectors = new NioThread[]{selector1, selector2};

        ILogger logger = mock(ILogger.class);
        when(logger.isFinestEnabled()).thenReturn(true);

        loadTracker = new LoadTracker(selectors, logger);
    }

    @Test
    public void testUpdateImbalance() throws Exception {
        MigratableHandler selector1Handler1 = mock(MigratableHandler.class);
        when(selector1Handler1.getLoad()).thenReturn(0L)
                .thenReturn(100L);
        when(selector1Handler1.getOwner())
                .thenReturn(selector1);
        loadTracker.addHandler(selector1Handler1);

        MigratableHandler selector2Handler1 = mock(MigratableHandler.class);
        when(selector2Handler1.getLoad())
                .thenReturn(0L)
                .thenReturn(200L);
        when(selector2Handler1.getOwner())
                .thenReturn(selector2);
        loadTracker.addHandler(selector2Handler1);

        MigratableHandler selector2Handler3 = mock(MigratableHandler.class);
        when(selector2Handler3.getLoad())
                .thenReturn(0L)
                .thenReturn(100L);
        when(selector2Handler3.getOwner())
                .thenReturn(selector2);
        loadTracker.addHandler(selector2Handler3);

        LoadImbalance loadImbalance = loadTracker.updateImbalance();
        assertEquals(0, loadImbalance.minimumEvents);
        assertEquals(0, loadImbalance.maximumEvents);

        loadTracker.updateImbalance();
        assertEquals(100, loadImbalance.minimumEvents);
        assertEquals(300, loadImbalance.maximumEvents);
        assertEquals(selector1, loadImbalance.destinationSelector);
        assertEquals(selector2, loadImbalance.sourceSelector);
    }

    // there is no point in selecting a selector with a single handler as source.
    @Test
    public void testUpdateImbalance_notUsingSingleHandlerSelectorAsSource() throws Exception {
        MigratableHandler selector1Handler1 = mock(MigratableHandler.class);
        // the first selector has a handler with a large number of events
        when(selector1Handler1.getLoad()).thenReturn(10000L);
        when(selector1Handler1.getOwner()).thenReturn(selector1);
        loadTracker.addHandler(selector1Handler1);

        MigratableHandler selector2Handler = mock(MigratableHandler.class);
        when(selector2Handler.getLoad()).thenReturn(200L);
        when(selector2Handler.getOwner()).thenReturn(selector2);
        loadTracker.addHandler(selector2Handler);

        MigratableHandler selector2Handler2 = mock(MigratableHandler.class);
        when(selector2Handler2.getLoad()).thenReturn(200L);
        when(selector2Handler2.getOwner()).thenReturn(selector2);
        loadTracker.addHandler(selector2Handler2);

        LoadImbalance loadImbalance = loadTracker.updateImbalance();

        assertEquals(400, loadImbalance.minimumEvents);
        assertEquals(400, loadImbalance.maximumEvents);
        assertEquals(selector2, loadImbalance.destinationSelector);
        assertEquals(selector2, loadImbalance.sourceSelector);
    }
}
