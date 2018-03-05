/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.core;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.test.PacketFiltersUtil.dropOperationsBetween;
import static com.hazelcast.test.PacketFiltersUtil.resetPacketFiltersFrom;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
public class CancellationTest extends JetTestSupport {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() {
        StuckSource.callCounter.set(0);
        FaultyProcessor.failNow = false;
        BlockingProcessor.hasStarted = false;
        BlockingProcessor.isDone = false;
    }

    @Test
    public void when_jobCancelledOnSingleNode_then_terminatedEventually() {
        // Given
        JetInstance instance = newInstance();

        DAG dag = new DAG();
        dag.newVertex("slow", StuckSource::new);

        Job job = instance.newJob(dag);
        assertExecutionStarted();

        // When
        job.cancel();

        // Then
        assertExecutionTerminated();
        expectedException.expect(CancellationException.class);
        job.join();
    }

    @Test
    public void when_jobCancelledOnMultipleNodes_then_terminatedEventually() {
        // Given
        newInstance();
        JetInstance instance = newInstance();

        DAG dag = new DAG();
        dag.newVertex("slow", StuckSource::new);

        Job job = instance.newJob(dag);
        assertExecutionStarted();

        // When
        job.cancel();

        // Then
        assertExecutionTerminated();
        expectedException.expect(CancellationException.class);
        job.join();
    }

    @Test
    public void when_jobCancelled_then_jobStatusIsSetEventually() {
        // Given
        JetInstance instance = newInstance();

        DAG dag = new DAG();
        dag.newVertex("slow", StuckSource::new);

        Job job = instance.newJob(dag);
        assertExecutionStarted();

        // When
        job.cancel();

        // Then
        assertTrueEventually(() -> assertEquals(JobStatus.COMPLETED, job.getStatus()), 3);
    }

    @Test
    public void when_jobCancelledFromClient_then_terminatedEventually() {
        // Given
        newInstance();
        newInstance();
        JetInstance client = createJetClient();

        DAG dag = new DAG();
        dag.newVertex("slow", StuckSource::new);

        Job job = client.newJob(dag);
        assertExecutionStarted();

        // When
        job.cancel();

        // Then
        assertExecutionTerminated();
        expectedException.expect(CancellationException.class);
        job.join();
    }

    @Test
    public void when_jobCancelledFromClient_then_jobStatusIsSetEventually() {
        // Given
        newInstance();
        newInstance();
        JetInstance client = createJetClient();

        DAG dag = new DAG();
        dag.newVertex("slow", StuckSource::new);

        Job job = client.newJob(dag);
        assertExecutionStarted();

        // When
        job.cancel();

        // Then
        assertTrueEventually(() -> assertEquals(JobStatus.COMPLETED, job.getStatus()), 3);
    }

    @Test
    public void when_jobCancelled_then_trackedJobsGetNotified() {
        // Given
        JetInstance instance1 = newInstance();
        JetInstance instance2 = newInstance();

        DAG dag = new DAG();
        dag.newVertex("slow", StuckSource::new);

        Job job = instance1.newJob(dag);
        assertExecutionStarted();

        // When
        job.cancel();

        // Then
        assertExecutionTerminated();
        expectedException.expect(CancellationException.class);
        Job tracked = instance2.getJobs().iterator().next();
        tracked.join();
    }

    @Test
    public void when_jobCancelled_then_jobStatusIsSetDuringCancellation() {
        // Given
        JetInstance instance1 = newInstance();
        JetInstance instance2 = newInstance();
        dropOperationsBetween(instance1.getHazelcastInstance(), instance2.getHazelcastInstance(),
                JetInitDataSerializerHook.FACTORY_ID, singletonList(JetInitDataSerializerHook.COMPLETE_EXECUTION_OP));

        DAG dag = new DAG();
        dag.newVertex("slow", StuckSource::new);

        Job job = instance1.newJob(dag);
        assertExecutionStarted();

        // When
        job.cancel();

        // Then
        assertTrueEventually(() -> assertEquals(JobStatus.COMPLETING, job.getStatus()), 3);

        resetPacketFiltersFrom(instance1.getHazelcastInstance());

        assertTrueEventually(() -> assertEquals(JobStatus.COMPLETED, job.getStatus()), 3);
    }

    @Test
    public void when_jobFailsOnOnInitiatorNode_then_cancelledOnOtherNodes() throws Throwable {
        // Given
        JetInstance instance = newInstance();
        newInstance();

        RuntimeException fault = new RuntimeException("fault");
        DAG dag = new DAG();

        SingleNodeFaultSupplier supplier = new SingleNodeFaultSupplier(getAddress(instance.getHazelcastInstance()), fault);
        dag.newVertex("faulty", supplier).localParallelism(4);

        Job job = instance.newJob(dag);
        assertExecutionStarted();

        // Then
        FaultyProcessor.failNow = true;
        assertExecutionTerminated();

        expectedException.expect(fault.getClass());
        expectedException.expectMessage(fault.getMessage());
        try {
            job.join();
        } catch (Exception e) {
            throw peel(e);
        }
    }

    @Test
    public void when_jobFailsOnOnNonInitiatorNode_then_cancelledOnInitiatorNode() throws Throwable {
        // Given
        JetInstance instance = newInstance();
        JetInstance other = newInstance();

        RuntimeException fault = new RuntimeException("fault");
        DAG dag = new DAG();
        dag.newVertex("faulty", new SingleNodeFaultSupplier(getAddress(other.getHazelcastInstance()), fault))
           .localParallelism(4);

        Job job = instance.newJob(dag);
        assertExecutionStarted();

        // Then
        FaultyProcessor.failNow = true;
        assertExecutionTerminated();

        expectedException.expect(fault.getClass());
        expectedException.expectMessage(fault.getMessage());
        try {
            job.join();
        } catch (Exception e) {
            throw peel(e);
        }
    }

    @Test
    public void when_shutdown_then_jobFuturesCanceled() throws Exception {
        JetInstance jet = newInstance();
        DAG dag = new DAG();
        dag.newVertex("blocking", CloseableProcessorSupplier.of(BlockingProcessor::new)).localParallelism(1);
        jet.newJob(dag);
        assertTrueEventually(() -> assertTrue(BlockingProcessor.hasStarted), 3);
        jet.shutdown();
        assertBlockingProcessorEventuallyNotRunning();
    }

    @Test
    public void when_jobCanceled_then_jobFutureCanceled() {
        JetInstance jet = newInstance();
        DAG dag = new DAG();
        dag.newVertex("blocking", CloseableProcessorSupplier.of(BlockingProcessor::new)).localParallelism(1);
        Job job = jet.newJob(dag);
        assertTrueEventually(() -> assertTrue(BlockingProcessor.hasStarted), 3);
        job.cancel();
        assertBlockingProcessorEventuallyNotRunning();
    }

    private JetInstance newInstance() {
        return createJetMember();
    }

    private static void assertExecutionStarted() {
        final long first = StuckSource.callCounter.get();
        assertTrueEventually(() -> assertTrue("Call counter should eventually start being incremented.",
                first != StuckSource.callCounter.get()), 3);
    }

    private static void assertExecutionTerminated() {
        final long[] previous = {0};
        assertTrueEventually(() -> {
                long current = StuckSource.callCounter.get();
                long last = previous[0];
                previous[0] = current;
                assertTrue("Call counter should eventually stop being incremented.", current == last);
            }, 3);
    }

    private static void assertBlockingProcessorEventuallyNotRunning() {
        assertTrueEventually(() -> assertTrue(
                String.format("BlockingProcessor should be started and done; hasStarted=%b, isDone=%b",
                        BlockingProcessor.hasStarted, BlockingProcessor.isDone),
                BlockingProcessor.hasStarted && BlockingProcessor.isDone), 3);
    }

    private static class StuckSource extends AbstractProcessor {

        static final AtomicLong callCounter = new AtomicLong();

        @Override
        public boolean complete() {
            callCounter.incrementAndGet();
            sleepMillis(1);
            return false;
        }
    }

    private static class BlockingProcessor extends AbstractProcessor implements Closeable {

        static volatile boolean hasStarted;
        static volatile boolean isDone;

        BlockingProcessor() {
            setCooperative(false);
        }

        @Override
        public boolean complete() {
            hasStarted = true;
            return false;
        }

        @Override
        public void close() {
            isDone = true;
        }
    }

    private static class FaultyProcessor extends AbstractProcessor {

        static volatile boolean failNow;

        private final RuntimeException e;

        FaultyProcessor(@Nonnull RuntimeException e) {
            this.e = e;
        }

        @Override
        public boolean complete() {
            if (failNow) {
                throw e;
            } else {
                return false;
            }
        }
    }

    private static class SingleNodeFaultSupplier implements ProcessorMetaSupplier {

        private String host;
        private int port;
        private RuntimeException e;

        SingleNodeFaultSupplier(Address failOnAddress, RuntimeException e) {
            this.e = e;
            this.host = failOnAddress.getHost();
            this.port = failOnAddress.getPort();
        }

        @Override @Nonnull
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            Address failOnAddress;
            try {
                failOnAddress = new Address(host, port);
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }

            return address ->
                    ProcessorSupplier.of(address.equals(failOnAddress)
                            ? () -> new FaultyProcessor(e)
                            : StuckSource::new);
        }
    }
}
