/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.collection.IQueue;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.collection.impl.queue.QueueService.SERVICE_NAME;
import static org.junit.Assert.assertEquals;


@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueueDestroyTest extends HazelcastTestSupport  {

    @Test
    public void checkStatsMapEntryRemovedWhenQueueDestroyed() {
        final int LOOP_COUNT = 10;
        final int ADD_COUNT = 100;

        HazelcastInstance hz = createHazelcastInstance(smallInstanceConfig());
        QueueService qService = Util.getNodeEngine(hz).getService(SERVICE_NAME);

        for (int i = 0; i < LOOP_COUNT; i++) {
            String queueName = String.valueOf(i);
            IQueue<Integer> q = hz.getQueue(queueName);

            for (int j = 0; j < ADD_COUNT; j++) {
                q.add(j);
            }
            q.destroy();
            assertEquals(0, qService.getStatsMap().size());
        }
    }
}
