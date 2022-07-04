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

package com.hazelcast.internal.networking.nio;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentMatchers;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.runners.Parameterized.UseParametersRunnerFactory;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

// Tests configuration of write-through and selection key wake-up optimizations
// with different selector modes.
@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NioNetworkingConfigurationTest {

    @Parameterized.Parameters(name = "selectorMode={0},configured={1},expected={2}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]
                {
                        // selector mode, configured value, expected value
                        {SelectorMode.SELECT, false, false},
                        {SelectorMode.SELECT, true, true},
                        {SelectorMode.SELECT_NOW, false, false},
                        {SelectorMode.SELECT_NOW, true, true},
                        {SelectorMode.SELECT_WITH_FIX, false, false},
                        {SelectorMode.SELECT_WITH_FIX, true, false},
                }
        );
    }

    @Parameterized.Parameter
    public SelectorMode selectorMode;

    @Parameterized.Parameter(1)
    public boolean configuredValue;

    @Parameterized.Parameter(2)
    public boolean expectedValue;

    private NioNetworking.Context ctx;
    private LoggingService loggingService;
    private ILogger logger;

    @Before
    public void setup() {
        loggingService = mock(LoggingService.class);
        logger = mock(ILogger.class);
        when(loggingService.getLogger(ArgumentMatchers.any(Class.class)))
                .thenReturn(logger);

        ctx = new NioNetworking.Context();
        ctx.loggingService(loggingService);
    }

    @Test
    public void testOptimizationsConfiguration() {
        ctx.writeThroughEnabled(configuredValue)
           .selectionKeyWakeupEnabled(configuredValue)
           .selectorMode(selectorMode);
        NioNetworking networking = new NioNetworking(ctx);

        Assert.assertEquals(expectedValue, networking.isSelectionKeyWakeupEnabled());
        Assert.assertEquals(expectedValue, networking.isWriteThroughEnabled());
    }
}
