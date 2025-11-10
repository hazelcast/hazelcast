/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.InvocationUtil;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InvokeAndReduceOnMembersTest extends HazelcastTestSupport {

    private static final Duration TIMEOUT = Duration.ofSeconds(ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    private static final int NODE_COUNT = 3;
    private NodeEngine nodeEngine;

    private final BiFunction<Object, Throwable, Object> map = (res, t) -> t == null ? (int) res : 0;
    private final Function<Map<Member, Object>, Integer> reduce = m -> m.values().stream().mapToInt(i -> (int) i).sum();

    @Before
    public void setup() {
        HazelcastInstance[] cluster = createHazelcastInstances(NODE_COUNT);
        nodeEngine = getNodeEngineImpl(cluster[0]);
    }

    @Test
    public void success() throws ExecutionException, InterruptedException {
        var future = InvocationUtil.invokeAndReduceOnAllClusterMembers(
                nodeEngine,
                SuccessfulOperation::new,
                map,
                reduce
        );
        assertThat(future.get()).isEqualTo(NODE_COUNT);
    }

    @Test
    public void oneFails() throws ExecutionException, InterruptedException {
        var future = InvocationUtil.invokeAndReduceOnAllClusterMembers(
                nodeEngine,
                MasterFailsOperation::new,
                map,
                reduce
        );
        assertThat(future.get()).isEqualTo(NODE_COUNT - 1);
    }

    @Test
    public void allFails() throws ExecutionException, InterruptedException {
        var future = InvocationUtil.invokeAndReduceOnAllClusterMembers(
                nodeEngine,
                AllFailsOperation::new,
                map,
                reduce
        );
        assertThat(future.get()).isEqualTo(0);
    }

    @Test
    public void mapThrowsException() {
        var future = InvocationUtil.invokeAndReduceOnAllClusterMembers(
                nodeEngine,
                SuccessfulOperation::new,
                (k, t) -> {
                    throw new RuntimeException("Mapping failed");
                },
                reduce
        );
        assertThat(future).completesExceptionallyWithin(TIMEOUT);
    }

    @Test
    public void reduceThrowsException() {
        var future = InvocationUtil.invokeAndReduceOnAllClusterMembers(
                nodeEngine,
                SuccessfulOperation::new,
                map,
                (map) -> {
                    throw new RuntimeException("Reducing failed");
                }
        );

        assertThat(future).completesExceptionallyWithin(TIMEOUT);
    }


    private static class SuccessfulOperation extends Operation {

        @Override
        public void run() {
            // no-op
        }

        @Override
        public Object getResponse() {
            return 1;
        }
    }

    private static class MasterFailsOperation extends Operation {

        @Override
        public void run() {
            if (getNodeEngine().getClusterService().isMaster()) {
                throw new RuntimeException("Simulated failure");
            }
        }

        @Override
        public Object getResponse() {
            return 1;
        }
    }

    private static class AllFailsOperation extends Operation {

        @Override
        public void run() {
            throw new RuntimeException("Simulated failure");

        }

        @Override
        public Object getResponse() {
            return 1;
        }
    }
}
