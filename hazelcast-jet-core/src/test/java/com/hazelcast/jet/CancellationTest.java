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

import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
@RunWith(HazelcastSerialClassRunner.class)
public class CancellationTest extends JetTestSupport {

    private static final int TIMEOUT_MILLIS = 8000;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private JetTestInstanceFactory factory;
    private JetConfig config;

    @Before
    public void setup() {
        config = new JetConfig();
        config.getHazelcastConfig().getProperties().put(GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(),
                Integer.toString(TIMEOUT_MILLIS));

        factory = new JetTestInstanceFactory();
        StuckProcessor.callCounter.set(0);
        FaultyProcessor.failNow = false;
        BlockingProcessor.hasStarted = false;
        BlockingProcessor.isDone = false;
    }

    @After
    public void shutdown() {
        factory.shutdownAll();
    }

    @Test
    public void when_jobCancelledOnSingleNode_then_terminatedEventually() throws Throwable {
        // Given
        JetInstance instance = newInstance();

        DAG dag = new DAG();
        dag.newVertex("slow", StuckProcessor::new);

        Future<Void> future = instance.newJob(dag).execute();
        assertExecutionStarted();

        // When
        future.cancel(true);

        // Then
        assertExecutionTerminated();
        expectedException.expect(CancellationException.class);
        future.get();
    }

    @Test
    public void when_jobCancelledOnMultipleNodes_then_terminatedEventually() throws Throwable {
        // Given
        newInstance();
        JetInstance instance = newInstance();

        DAG dag = new DAG();
        dag.newVertex("slow", StuckProcessor::new);

        Future<Void> future = instance.newJob(dag).execute();
        assertExecutionStarted();

        // When
        future.cancel(true);

        // Then
        assertExecutionTerminated();
        expectedException.expect(CancellationException.class);
        future.get();
    }

    @Test
    public void when_jobCancelledFromClient_then_terminatedEventually() throws Throwable {
        // Given
        newInstance();
        newInstance();
        JetInstance client = factory.newClient();

        DAG dag = new DAG();
        dag.newVertex("slow", StuckProcessor::new);

        Future<Void> future = client.newJob(dag).execute();
        assertExecutionStarted();

        // When
        future.cancel(true);

        // Then
        assertExecutionTerminated();
        expectedException.expect(CancellationException.class);
        future.get();
    }

    @Test
    public void when_jobFailsOnOnInitiatorNode_then_cancelledOnOtherNodes() throws Throwable {
        // Given
        JetInstance instance = newInstance();
        JetInstance other = newInstance();

        RuntimeException fault = new RuntimeException("fault");
        DAG dag = new DAG();

        SingleNodeFaultSupplier supplier = new SingleNodeFaultSupplier(getAddress(instance.getHazelcastInstance()), fault);
        dag.newVertex("faulty", supplier).localParallelism(4);

        Future<Void> future = instance.newJob(dag).execute();
        assertExecutionStarted();

        // Then
        FaultyProcessor.failNow = true;
        assertExecutionTerminated();

        expectedException.expect(fault.getClass());
        expectedException.expectMessage(fault.getMessage());
        try {
            future.get();
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

        Future<Void> future = instance.newJob(dag).execute();
        assertExecutionStarted();

        // Then
        FaultyProcessor.failNow = true;
        assertExecutionTerminated();

        expectedException.expect(fault.getClass());
        expectedException.expectMessage(fault.getMessage());
        try {
            future.get();
        } catch (Exception e) {
            throw peel(e);
        }
    }

    @Test
    public void when_shutdown_then_jobFuturesCanceled() throws Exception {
        JetInstance jet = newInstance();
        DAG dag = new DAG();
        dag.newVertex("blocking", BlockingProcessor::new).localParallelism(1);
        jet.newJob(dag).execute();
        jet.shutdown();
        Thread.sleep(3000);
        assertBlockingProcessorEventuallyNotRunning();
    }

    @Test
    public void when_jobCanceled_then_jobFutureCanceled() throws Exception {
        JetInstance jet = newInstance();
        DAG dag = new DAG();
        dag.newVertex("blocking", BlockingProcessor::new).localParallelism(1);
        jet.newJob(dag).execute().cancel(true);
        Thread.sleep(3000);
        assertBlockingProcessorEventuallyNotRunning();
    }

    private JetInstance newInstance() {
        return factory.newMember(config);
    }

    private static void assertExecutionStarted() {
        final long first = StuckProcessor.callCounter.get();
        assertTrueEventually(() -> assertTrue("Call counter should eventually start being incremented.",
                first != StuckProcessor.callCounter.get()), 5);
    }

    private static void assertExecutionTerminated() {
        final long[] previous = {0};
        assertTrueEventually(() -> {
                long current = StuckProcessor.callCounter.get();
                long last = previous[0];
                previous[0] = current;
                assertTrue("Call counter should eventually stop being incremented.", current == last);
            });
    }

    private void assertBlockingProcessorEventuallyNotRunning() {
        try {
            assertTrueEventually(() -> assertTrue(BlockingProcessor.hasStarted == BlockingProcessor.isDone), 40);
        } catch (AssertionError e) {
            System.err.format("Test failing. Blocking processor has started? %b isDone? %b%n",
                    BlockingProcessor.hasStarted, BlockingProcessor.isDone);
        }
    }

    private static class StuckProcessor extends AbstractProcessor {

        static final AtomicLong callCounter = new AtomicLong();

        @Override
        public boolean complete() {
            callCounter.incrementAndGet();
            sleepMillis(1);
            return false;
        }
    }

    private static class BlockingProcessor extends AbstractProcessor {

        static volatile boolean hasStarted;
        static volatile boolean isDone;

        private CompletableFuture<Void> jobFuture;

        BlockingProcessor() {
            setCooperative(false);
        }

        @Override
        protected void init(@Nonnull Context context) throws Exception {
            jobFuture = context.jobFuture();
        }

        @Override
        public boolean complete() {
            hasStarted = true;
            while (!jobFuture.isDone()) {
                parkNanos(MILLISECONDS.toNanos(200));
            }
            return isDone = true;
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

    private static class SingleNodeFaultSupplier implements DataSerializable, ProcessorMetaSupplier {

        private transient Address failOnAddress;
        private RuntimeException e;

        SingleNodeFaultSupplier(Address failOnAddress, RuntimeException e) {
            this.e = e;
            this.failOnAddress = failOnAddress;
        }

        @Override @Nonnull
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return address ->
                    ProcessorSupplier.of(address.equals(failOnAddress)
                            ? () -> new FaultyProcessor(e)
                            : StuckProcessor::new);
        }

        @Override
        public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
            objectDataOutput.writeObject(failOnAddress);
            objectDataOutput.writeObject(e);
        }

        @Override
        public void readData(ObjectDataInput objectDataInput) throws IOException {
            e = objectDataInput.readObject();
            failOnAddress = objectDataInput.readObject();
        }
    }
}
