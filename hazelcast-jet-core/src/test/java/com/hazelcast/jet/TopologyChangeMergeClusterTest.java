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

package com.hazelcast.jet;

import com.hazelcast.jet.TopologyChangeTest.MockSupplier;
import com.hazelcast.jet.TopologyChangeTest.StuckProcessor;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@Category(QuickTest.class)
@RunWith(HazelcastSerialClassRunner.class)
public class TopologyChangeMergeClusterTest extends JetSplitBrainTestSupport {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    private Future<Void> future;

    @Override
    protected int[] brains() {
        return new int[]{NODE_COUNT / 2, NODE_COUNT / 2};
    }

    @Override
    protected void onBeforeSetup() {
        MockSupplier.completeCount.set(0);
        MockSupplier.initCount.set(0);
        MockSupplier.completeErrors.clear();

        StuckProcessor.proceedLatch = new CountDownLatch(1);
        StuckProcessor.executionStarted = new CountDownLatch((NODE_COUNT / 2) * PARALLELISM);
    }

    @Override
    protected void onAfterSplitBrainCreated(JetInstance[] firstBrain, JetInstance[] secondBrain) throws Exception {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockSupplier(StuckProcessor::new, NODE_COUNT / 2)));

        future = firstBrain[0].newJob(dag).execute();
        StuckProcessor.executionStarted.await();
    }

    @Override
    protected void onAfterSplitBrainHealed(JetInstance[] instances) throws Exception {
        // When
        StuckProcessor.proceedLatch.countDown();
        future.get();

        // Then
        assertEquals(NODE_COUNT / 2, MockSupplier.initCount.get());

        assertTrueEventually(() -> {
            assertEquals(NODE_COUNT / 2, MockSupplier.completeCount.get());
            assertThat(MockSupplier.completeErrors, empty());
        });
    }
}
