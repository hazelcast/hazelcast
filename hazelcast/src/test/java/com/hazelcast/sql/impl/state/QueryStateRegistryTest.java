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
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.TestClockProvider;
import com.hazelcast.sql.impl.exec.root.BlockingRootResultConsumer;
import com.hazelcast.sql.impl.plan.Plan;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryStateRegistryTest extends SqlTestSupport {
    @Test
    public void testInitiatorState() {
        long currentTime = 50L;
        QueryStateRegistry registry = new QueryStateRegistry(TestClockProvider.createStatic(currentTime));

        UUID localMemberId = UUID.randomUUID();
        long timeout = 100L;
        Plan initiatorPlan = opaquePlan();
        QueryResultProducer resultProducer = new BlockingRootResultConsumer();
        QueryStateCompletionCallback completionCallback = new TestQueryStateCompletionCallback();

        // Test without registration.
        QueryState state = registry.onInitiatorQueryStarted(
            localMemberId,
            timeout,
            initiatorPlan,
            resultProducer,
            completionCallback,
            false
        );

        assertEquals(state.getQueryId().getMemberId(), localMemberId);
        assertEquals(state.getLocalMemberId(), localMemberId);
        assertEquals(state.getStartTime(), currentTime);

        assertTrue(state.isInitiator());
        assertEquals(state.getQueryId(), state.getInitiatorState().getQueryId());
        assertEquals(initiatorPlan, state.getInitiatorState().getPlan());
        assertEquals(resultProducer, state.getInitiatorState().getResultProducer());
        assertEquals(timeout, state.getInitiatorState().getTimeout());

        assertTrue(registry.getStates().isEmpty());

        // Test with registration.
        state = registry.onInitiatorQueryStarted(
            localMemberId,
            timeout,
            initiatorPlan,
            resultProducer,
            completionCallback,
            true
        );

        assertEquals(1, registry.getStates().size());
        assertSame(state, registry.getStates().iterator().next());
        assertSame(state, registry.getState(state.getQueryId()));

        // Add distributed state on top of existing initiator state.
        QueryState state2 = registry.onDistributedQueryStarted(localMemberId, state.getQueryId(), completionCallback);

        assertSame(state2, state);
    }

    @Test
    public void testDistributedState() {
        long currentTime = 50L;
        QueryStateRegistry registry = new QueryStateRegistry(TestClockProvider.createStatic(currentTime));

        QueryStateCompletionCallback completionCallback = new TestQueryStateCompletionCallback();

        UUID localMemberId = UUID.randomUUID();
        UUID remoteMemberId = UUID.randomUUID();

        QueryId queryId = QueryId.create(localMemberId);

        // Test missing initiator state.
        QueryState state = registry.onDistributedQueryStarted(localMemberId, queryId, completionCallback);

        assertNull(state);
        assertTrue(registry.getStates().isEmpty());

        // Test normal remote invocation.
        state = registry.onDistributedQueryStarted(remoteMemberId, queryId, completionCallback);

        assertEquals(1, registry.getStates().size());
        assertSame(state, registry.getStates().iterator().next());
        assertSame(state, registry.getState(state.getQueryId()));

        assertEquals(state.getQueryId(), queryId);
        assertEquals(state.getLocalMemberId(), remoteMemberId);
        assertEquals(state.getStartTime(), currentTime);

        assertFalse(state.isInitiator());
    }

    @Test
    public void testClear() {
        QueryStateRegistry registry = new QueryStateRegistry(TestClockProvider.createDefault());

        QueryStateCompletionCallback completionCallback = new TestQueryStateCompletionCallback();

        QueryId queryId = QueryId.create(UUID.randomUUID());

        // Test clear on completion.
        QueryState state = registry.onDistributedQueryStarted(UUID.randomUUID(), queryId, completionCallback);

        assertEquals(1, registry.getStates().size());
        assertSame(state, registry.getStates().iterator().next());
        assertSame(state, registry.getState(state.getQueryId()));

        registry.onQueryCompleted(queryId);

        assertNull(registry.getState(queryId));
        assertTrue(registry.getStates().isEmpty());

        // Test clear on reset.
        state = registry.onDistributedQueryStarted(UUID.randomUUID(), queryId, completionCallback);

        assertEquals(1, registry.getStates().size());
        assertSame(state, registry.getStates().iterator().next());
        assertSame(state, registry.getState(state.getQueryId()));

        registry.reset();

        assertNull(registry.getState(queryId));
        assertTrue(registry.getStates().isEmpty());
    }

    private static class TestQueryStateCompletionCallback implements QueryStateCompletionCallback {
        @Override
        public void onCompleted(QueryId queryId) {
            // No-op.
        }

        @Override
        public void onError(
            QueryId queryId,
            int errorCode,
            String errorMessage,
            UUID originatingMemberId,
            Collection<UUID> memberIds
        ) {
            // No-op.
        }
    }
}
