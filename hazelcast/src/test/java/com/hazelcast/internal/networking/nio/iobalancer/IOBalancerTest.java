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

package com.hazelcast.internal.networking.nio.iobalancer;

import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.networking.nio.MigratablePipeline;
import com.hazelcast.internal.networking.nio.NioThread;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.logging.impl.LoggingServiceImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IOBalancerTest {
    private final LoggingService loggingService = new LoggingServiceImpl("somegroup", "log4j2", BuildInfoProvider.getBuildInfo(), true, null);

    // https://github.com/hazelcast/hazelcast/issues/11501
    @Test
    public void whenChannelAdded_andDisabled_thenSkipTaskCreation() {
        IOBalancer ioBalancer = new IOBalancer(new NioThread[1], new NioThread[1], "foo", 1, loggingService);
        MigratablePipeline inboundPipeline = mock(MigratablePipeline.class);
        MigratablePipeline outboundPipeline = mock(MigratablePipeline.class);

        ioBalancer.channelAdded(inboundPipeline, outboundPipeline);

        assertTrue(ioBalancer.getWorkQueue().isEmpty());
    }

    // https://github.com/hazelcast/hazelcast/issues/11501
    @Test
    public void whenChannelRemoved_andDisabled_thenSkipTaskCreation() {
        IOBalancer ioBalancer = new IOBalancer(new NioThread[1], new NioThread[1], "foo", 1, loggingService);
        MigratablePipeline inboundPipeline = mock(MigratablePipeline.class);
        MigratablePipeline outboundPipelines = mock(MigratablePipeline.class);

        ioBalancer.channelRemoved(inboundPipeline, outboundPipelines);

        assertTrue(ioBalancer.getWorkQueue().isEmpty());
    }
}
