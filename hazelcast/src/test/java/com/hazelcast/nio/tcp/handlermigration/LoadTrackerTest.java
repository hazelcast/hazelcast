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

package com.hazelcast.nio.tcp.handlermigration;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.tcp.AbstractIOSelector;
import com.hazelcast.nio.tcp.MigratableHandler;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class LoadTrackerTest {

    private AbstractIOSelector selector1;
    private AbstractIOSelector selector2;
    private AbstractIOSelector[] selectors;
    private LoadTracker loadTracker;

    @Before
    public void setUp() {
        selector1 = mock(AbstractIOSelector.class);
        selector2 = mock(AbstractIOSelector.class);
        selectors = new AbstractIOSelector[]{selector1, selector2};

        ILogger logger = mock(ILogger.class);
        when(logger.isFinestEnabled())
                .thenReturn(true);

        LoggingService loggingService = mock(LoggingService.class);
        when(loggingService.getLogger(LoadTracker.class))
                .thenReturn(logger);

        loadTracker = new LoadTracker(selectors, loggingService);
    }

    @Test
    public void testUpdateImbalance() throws Exception {
        MigratableHandler handler1 = mock(MigratableHandler.class);
        when(handler1.getEventCount())
                .thenReturn(0l)
                .thenReturn(100l);
        when(handler1.getOwner())
                .thenReturn(selector1);
        loadTracker.addHandler(handler1);

        MigratableHandler handler2 = mock(MigratableHandler.class);
        when(handler2.getEventCount())
                .thenReturn(0l)
                .thenReturn(200l);
        when(handler2.getOwner())
                .thenReturn(selector2);
        loadTracker.addHandler(handler2);

        MigratableHandler handler3 = mock(MigratableHandler.class);
        when(handler3.getEventCount())
                .thenReturn(0l)
                .thenReturn(100l);
        when(handler3.getOwner())
                .thenReturn(selector2);
        loadTracker.addHandler(handler3);

        LoadImbalance loadImbalance = loadTracker.updateImbalance();
        assertEquals(0, loadImbalance.minimumEvents);
        assertEquals(0, loadImbalance.maximumEvents);

        loadTracker.updateImbalance();
        assertEquals(100, loadImbalance.minimumEvents);
        assertEquals(300, loadImbalance.maximumEvents);
        assertEquals(selector1, loadImbalance.destinationSelector);
        assertEquals(selector2, loadImbalance.sourceSelector);
    }
}