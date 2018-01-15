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

package com.hazelcast.jet.core;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.nio.Address;
import com.hazelcast.test.ExpectedRuntimeException;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.traverseArray;
import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.core.ProcessorMetaSupplier.dontParallelize;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestProcessors {

    public static class Identity extends AbstractProcessor {
        @Override
        protected boolean tryProcess(int ordinal, @Nonnull Object item) {
            return tryEmit(item);
        }
    }

    public static class ProcessorThatFailsInComplete implements Processor {

        private final RuntimeException e;

        public ProcessorThatFailsInComplete(@Nonnull RuntimeException e) {
            this.e = e;
        }

        @Override
        public boolean complete() {
            throw e;
        }
    }

    public static class ProcessorThatFailsInInit extends AbstractProcessor {

        private final Exception e;

        ProcessorThatFailsInInit(@Nonnull Exception e) {
            this.e = e;
        }

        @Override
        protected void init(@Nonnull Context context) throws Exception {
            throw e;
        }
    }

    public static final class StuckProcessor implements Processor {
        public static volatile CountDownLatch executionStarted;
        public static volatile CountDownLatch proceedLatch;

        // how long time to wait during calls to complete()
        private final long timeoutMillis;

        public StuckProcessor() {
            this(1);
        }

        public StuckProcessor(long timeoutMillis) {
            this.timeoutMillis = timeoutMillis;
        }

        @Override
        public boolean complete() {
            executionStarted.countDown();
            try {
                return proceedLatch.await(timeoutMillis, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                return false;
            }
        }
    }

    public static final class StuckForeverSourceP implements Processor {
        @Override
        public boolean complete() {
            return false;
        }
    }

    public static class FailingOnCompletePS implements ProcessorSupplier {

        private final DistributedSupplier<Processor> supplierFn;

        FailingOnCompletePS(DistributedSupplier<Processor> supplierFn) {
            this.supplierFn = supplierFn;
        }

        @Override
        public void init(@Nonnull Context context) {

        }

        @Nonnull @Override
        public Collection<? extends Processor> get(int count) {
            return Stream.generate(supplierFn).limit(count).collect(toList());
        }

        @Override
        public void complete(Throwable error) {
            throw new ExpectedRuntimeException();
        }
    }

    public static class FailingOnCompletePMS implements ProcessorMetaSupplier {

        private final DistributedSupplier<ProcessorSupplier> supplierFn;

        public FailingOnCompletePMS(DistributedSupplier<ProcessorSupplier> supplierFn) {
            this.supplierFn = supplierFn;
        }

        @Override
        public void init(@Nonnull Context context) {

        }

        @Nonnull @Override
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return a -> supplierFn.get();
        }

        @Override
        public void complete(Throwable error) {
            throw new ExpectedRuntimeException();
        }
    }

    public static class MockPMS implements ProcessorMetaSupplier {

        static AtomicBoolean initCalled = new AtomicBoolean();
        static AtomicBoolean completeCalled = new AtomicBoolean();
        static AtomicReference<Throwable> completeError = new AtomicReference<>();

        private final RuntimeException initError;
        private final DistributedSupplier<MockPS> supplierFn;

        public MockPMS(DistributedSupplier<MockPS> supplierFn) {
            this(null, supplierFn);
        }

        public MockPMS(RuntimeException initError, DistributedSupplier<MockPS> supplierFn) {
            this.initError = initError;
            this.supplierFn = supplierFn;
        }

        @Override
        public void init(@Nonnull Context context) {
            assertTrue("PMS.init() already called once",
                    initCalled.compareAndSet(false, true)
            );
            if (initError != null) {
                throw initError;
            }
        }

        @Nonnull @Override
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return a -> supplierFn.get();
        }

        @Override
        public void complete(Throwable error) {
            assertEquals("all PS that have been init should have been completed at this point",
                    MockPS.initCount.get(), MockPS.completeCount.get());
            assertTrue("Complete called without calling init()", initCalled.get());
            assertTrue("PMS.complete() already called once",
                    completeCalled.compareAndSet(false, true)
            );
            assertTrue("PMS.complete() already called once",
                    completeError.compareAndSet(null, error)
            );
        }
    }

    public static class MockPS implements ProcessorSupplier {

        static AtomicInteger initCount = new AtomicInteger();
        static AtomicInteger completeCount = new AtomicInteger();
        static List<Throwable> completeErrors = new CopyOnWriteArrayList<>();

        private final RuntimeException initError;
        private final DistributedSupplier<Processor> supplier;
        private final int nodeCount;

        private boolean initCalled;

        MockPS(DistributedSupplier<Processor> supplier, int nodeCount) {
            this(null, supplier, nodeCount);
        }

        MockPS(RuntimeException initError, DistributedSupplier<Processor> supplier, int nodeCount) {
            this.initError = initError;
            this.supplier = supplier;
            this.nodeCount = nodeCount;
        }

        @Override
        public void init(@Nonnull Context context) {
            initCalled = true;
            initCount.incrementAndGet();

            if (initError != null) {
                throw initError;
            }
        }

        @Nonnull @Override
        public List<Processor> get(int count) {
            return Stream.generate(supplier).limit(count).collect(toList());
        }

        @Override
        public void complete(Throwable error) {
            if (error != null) {
                completeErrors.add(error);
            }
            completeCount.incrementAndGet();

            assertTrue("Complete called without calling init()", initCalled);
            assertTrue("Complete called without init being called on all the nodes! init count: "
                    + initCount.get() + " node count: " + nodeCount, initCount.get() >= nodeCount);
            assertTrue("Complete called " + completeCount.get() + " but init called "
                    + initCount.get() + " times!", completeCount.get() <= initCount.get());
        }
    }

    /**
     * A processor that emits the given list of items. The same items are
     * emitted from each instance.
     */
    public static class ListSource extends AbstractProcessor {
        private final Traverser<?> trav;

        public ListSource(List<?> list) {
            trav = traverseIterable(list);
        }

        public ListSource(Object ... list) {
            trav = traverseArray(list);
        }

        @Override
        public boolean complete() {
            return emitFromTraverser(trav);
        }

        /**
         * Returns meta-supplier with default local parallelism of 1
         */
        public static ProcessorMetaSupplier supplier(List<?> list) {
            return dontParallelize(() -> new ListSource(list));
        }
    }

    /**
     * A processor that maps Watermarks to String (otherwise, they would not be
     * inserted to sink). It passes other items without change (from all input
     * edges to all output edges. It can't be done using {@link
     * com.hazelcast.jet.core.processor.Processors#mapP} because it doesn't
     * handle watermarks.
     */
    public static class MapWatermarksToString extends AbstractProcessor {

        @Override
        protected boolean tryProcess(int ordinal, @Nonnull Object item) {
            return tryEmit(item);
        }

        @Override
        public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
            return tryEmit("wm(" + watermark.timestamp() + ')');
        }
    }
}
