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

package com.hazelcast.collection.impl.queue;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.collection.IQueue;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.function.Supplier;

import static com.hazelcast.collection.impl.queue.QueueService.SERVICE_NAME;
import static org.junit.Assert.assertEquals;


@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueueDestroyTest extends HazelcastTestSupport {

    private static final int LOOP_COUNT = 10;
    private static final int ADD_COUNT = 100;

    private static final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @Parameterized.Parameter
    public Supplier<HazelcastInstance> instanceSupplier;

    @Parameterized.Parameters(name = "instance={0}")
    public static Supplier<HazelcastInstance>[] parameters() {
        return new Supplier[]{hazelcastFactory::newHazelcastClient,
                () -> hazelcastFactory.newHazelcastInstance(smallInstanceConfig())};
    }

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void checkStatsMapEntryRemovedWhenQueueDestroyed() {
        HazelcastInstance member = hazelcastFactory.newHazelcastInstance(smallInstanceConfig());
        HazelcastInstance destroyerInstance = instanceSupplier.get();
        QueueService qService = Accessors.getService(member, SERVICE_NAME);

        for (int i = 0; i < LOOP_COUNT; i++) {
            String queueName = String.valueOf(i);
            IQueue<Integer> q = destroyerInstance.getQueue(queueName);

            for (int j = 0; j < ADD_COUNT; j++) {
                q.add(j);
            }
            q.destroy();
            assertEquals(0, qService.getStatsMap().size());
        }
    }
}
