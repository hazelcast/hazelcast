/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.state;

import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.QueryResultProducer;
import com.hazelcast.sql.impl.exec.root.BlockingRootResultConsumer;
import com.hazelcast.sql.impl.plan.Plan;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryInitiatorStateTest {
    @Test
    public void testInitiatorState() {
        QueryId queryId = QueryId.create(UUID.randomUUID());
        Plan plan = new Plan(null, null, null, null, null, null, null);
        QueryResultProducer resultProducer = new BlockingRootResultConsumer();
        long timeout = 1000L;

        QueryInitiatorState state = new QueryInitiatorState(queryId, plan, null, resultProducer, timeout);

        assertEquals(queryId, state.getQueryId());
        assertSame(plan, state.getPlan());
        assertSame(resultProducer, state.getResultProducer());
        assertEquals(timeout, state.getTimeout());
    }
}
