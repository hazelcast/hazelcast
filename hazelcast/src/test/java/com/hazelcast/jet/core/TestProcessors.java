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

package com.hazelcast.jet.core;

import com.hazelcast.cluster.Address;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.processor.Processors;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.traverseArray;
import static com.hazelcast.jet.Traversers.traverseItems;
import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.core.ProcessorMetaSupplier.preferLocalParallelismOne;
import static com.hazelcast.jet.impl.JetEvent.jetEvent;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class TestProcessors {

    private TestProcessors() { }

    /**
     * Reset the static counters in test processors. Call before starting each
     * test that uses them.
     */
    public static void reset(int totalParallelism) {
        MockPMS.initCalled.set(false);
        MockPMS.closeCalled.set(false);
        MockPMS.receivedCloseError.set(null);

        MockPS.closeCount.set(0);
        MockPS.initCount.set(0);
        MockPS.receivedCloseErrors.clear();

        MockP.initCount.set(0);
        MockP.closeCount.set(0);
        MockP.saveToSnapshotCalled = false;
        MockP.onSnapshotCompletedCalled = false;

        NoOutputSourceP.proceedLatch = new CountDownLatch(1);
        NoOutputSourceP.executionStarted = new CountDownLatch(totalParallelism);
        NoOutputSourceP.initCount.set(0);
        NoOutputSourceP.failure.set(null);

        DummyStatefulP.parallelism = totalParallelism;
        DummyStatefulP.wasRestored = true;

        CollectPerProcessorSink.lists = null;
    }

    public static DAG batchDag() {
        DAG dag = new DAG();
        dag.newVertex("v", MockP::new);
        return dag;
    }

    public static DAG streamingDag() {
        DAG dag = new DAG();
        dag.newVertex("v", () -> new MockP().streaming());
        return dag;
    }

    public static class Identity extends AbstractProcessor {
        @Override
        protected boolean tryProcess(int ordinal, @Nonnull Object item) {
            return tryEmit(item);
        }
    }

    /**
     * A source processor (stream or batch) that outputs no items and allows to
     * externally control when and whether to complete or fail.
     */
    public static final class NoOutputSourceP extends AbstractProcessor {
        public static volatile CountDownLatch executionStarted;
        public static volatile CountDownLatch proceedLatch;
        public static final AtomicReference<RuntimeException> failure = new AtomicReference<>();
        public static final AtomicInteger initCount = new AtomicInteger();

        // how long time to wait during calls to complete()
        private final long timeoutMillis;
        private boolean executionStartCountedDown;

        public NoOutputSourceP() {
            this(1);
        }

        public NoOutputSourceP(long timeoutMillis) {
            this.timeoutMillis = timeoutMillis;
        }

        @Override
        protected void init(@Nonnull Context context) throws Exception {
            initCount.incrementAndGet();
        }

        @Override
        public boolean complete() {
            if (!executionStartCountedDown) {
                executionStarted.countDown();
                executionStartCountedDown = true;
            }
            try {
                RuntimeException localFailure = failure.getAndUpdate(e -> null);
                if (localFailure != null) {
                    throw localFailure;
                }
                return proceedLatch.await(timeoutMillis, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                return false;
            }
        }
    }

    public static class MockPMS implements ProcessorMetaSupplier {

        static AtomicBoolean initCalled = new AtomicBoolean();
        static AtomicBoolean closeCalled = new AtomicBoolean();
        static AtomicReference<Throwable> receivedCloseError = new AtomicReference<>();

        private Throwable initError;
        private Throwable getError;
        private Throwable closeError;
        private final SupplierEx<ProcessorSupplier> supplierFn;

        public MockPMS(SupplierEx<ProcessorSupplier> supplierFn) {
            this.supplierFn = supplierFn;
        }

        public MockPMS setInitError(Throwable initError) {
            this.initError = initError;
            return this;
        }

        public MockPMS setGetError(Throwable getError) {
            this.getError = getError;
            return this;
        }

        public MockPMS setCloseError(Throwable closeError) {
            this.closeError = closeError;
            return this;
        }

        @Override
        public void init(@Nonnull Context context) {
            assertTrue("PMS.init() already called once",
                    initCalled.compareAndSet(false, true)
            );
            if (initError != null) {
                throw sneakyThrow(initError);
            }
        }

        @Nonnull @Override
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            if (getError != null) {
                throw sneakyThrow(getError);
            }
            return a -> supplierFn.get();
        }

        @Override
        public void close(Throwable error) {
            assertEquals("all PS that have been init should have been closed at this point",
                    MockPS.initCount.get(), MockPS.closeCount.get());
            assertTrue("Close called without calling init()", initCalled.get());
            assertTrue("PMS.close() already called once",
                    closeCalled.compareAndSet(false, true)
            );
            assertTrue("PMS.close() already called once",
                    receivedCloseError.compareAndSet(null, error)
            );

            if (closeError != null) {
                throw sneakyThrow(closeError);
            }
        }
    }

    public static class MockPS implements ProcessorSupplier {

        static AtomicInteger initCount = new AtomicInteger();
        static AtomicInteger closeCount = new AtomicInteger();
        static List<Throwable> receivedCloseErrors = new CopyOnWriteArrayList<>();

        private Throwable initError;
        private Throwable getError;
        private Throwable closeError;

        private final SupplierEx<Processor> supplier;
        private final int nodeCount;

        private boolean initCalled;

        public MockPS(SupplierEx<Processor> supplier, int nodeCount) {
            this.supplier = supplier;
            this.nodeCount = nodeCount;
        }

        public MockPS setInitError(Throwable initError) {
            this.initError = initError;
            return this;
        }

        public MockPS setGetError(Throwable getError) {
            this.getError = getError;
            return this;
        }

        public MockPS setCloseError(Throwable closeError) {
            this.closeError = closeError;
            return this;
        }

        @Override
        public void init(@Nonnull Context context) {
            initCalled = true;
            initCount.incrementAndGet();

            if (initError != null) {
                throw sneakyThrow(initError);
            }
        }

        @Nonnull @Override
        public List<Processor> get(int count) {
            if (getError != null) {
                throw sneakyThrow(getError);
            }
            return Stream.generate(supplier).limit(count).collect(toList());
        }

        @Override
        public void close(Throwable error) {
            if (error != null) {
                receivedCloseErrors.add(error);
            }
            closeCount.incrementAndGet();

            assertTrue("Close called without calling init()", initCalled);
            assertTrue("Close called without init being called on all the nodes. Init count: "
                    + initCount.get() + " node count: " + nodeCount, initCount.get() >= nodeCount);
            assertTrue("Close called " + closeCount.get() + " times, but init called "
                    + initCount.get() + " times!", closeCount.get() <= initCount.get());

            if (closeError != null) {
                throw sneakyThrow(closeError);
            }
        }
    }

    public static class MockP extends AbstractProcessor {

        static AtomicInteger initCount = new AtomicInteger();
        static AtomicInteger closeCount = new AtomicInteger();
        static volatile boolean onSnapshotCompletedCalled;
        static volatile boolean saveToSnapshotCalled;

        private Throwable initError;
        private Throwable processError;
        private Throwable completeError;
        private Throwable closeError;
        private Throwable onSnapshotCompleteError;
        private Throwable saveToSnapshotError;
        private boolean isCooperative;
        private boolean streaming;

        @Override
        public boolean isCooperative() {
            return isCooperative;
        }

        public MockP setInitError(Throwable initError) {
            this.initError = initError;
            return this;
        }

        public MockP setProcessError(Throwable processError) {
            this.processError = processError;
            return this;
        }

        public MockP setCompleteError(Throwable completeError) {
            this.completeError = completeError;
            return this;
        }

        public MockP setOnSnapshotCompleteError(Throwable e) {
            this.onSnapshotCompleteError = e;
            return this;
        }

        public MockP setSaveToSnapshotError(Throwable e) {
            this.saveToSnapshotError = e;
            return this;
        }

        public MockP setCloseError(Throwable closeError) {
            this.closeError = closeError;
            return this;
        }

        public MockP nonCooperative() {
            isCooperative = false;
            return this;
        }

        public MockP streaming() {
            streaming = true;
            return this;
        }

        @Override
        protected void init(@Nonnull Context context) {
            initCount.incrementAndGet();
            if (initError != null) {
                throw sneakyThrow(initError);
            }
        }

        @Override
        protected boolean tryProcess(int ordinal, @Nonnull Object item) {
            if (processError != null) {
                throw sneakyThrow(processError);
            }
            return tryEmit(item);
        }

        @Override
        public boolean complete() {
            if (completeError != null) {
                throw sneakyThrow(completeError);
            }
            return !streaming;
        }

        @Override
        public boolean saveToSnapshot() {
            saveToSnapshotCalled = true;
            if (saveToSnapshotError != null) {
                throw sneakyThrow(saveToSnapshotError);
            }
            return true;
        }

        @Override
        public boolean snapshotCommitFinish(boolean success) {
            onSnapshotCompletedCalled = true;
            if (onSnapshotCompleteError != null) {
                throw sneakyThrow(onSnapshotCompleteError);
            }
            return true;
        }

        @Override
        public void close() {
            closeCount.incrementAndGet();
            if (closeError != null) {
                throw sneakyThrow(closeError);
            }
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
            return preferLocalParallelismOne(() -> new ListSource(list));
        }
    }

    /**
     * A processor that maps Watermarks to String (otherwise, they would not be
     * inserted to sink). It passes other items without change (from all input
     * edges to all output edges, including the watermarks. It can't be done
     * using {@link Processors#mapP} because it doesn't handle watermarks.
     */
    public static final class MapWatermarksToString extends AbstractProcessor {

        private final FlatMapper<Watermark, Object> flatMapper;

        private MapWatermarksToString(boolean wrapToJetEvent) {
            this.flatMapper = wrapToJetEvent
                    ? flatMapper(wm -> traverseItems(jetEvent(wm.timestamp(), "wm(" + wm.timestamp() + ')'), wm))
                    : flatMapper(wm -> traverseItems("wm(" + wm.timestamp() + ')', wm));
        }

        public static SupplierEx<Processor> mapWatermarksToString(boolean wrapToJetEvent) {
            return () -> new MapWatermarksToString(wrapToJetEvent);
        }

        @Override
        protected boolean tryProcess(int ordinal, @Nonnull Object item) {
            return tryEmit(item);
        }

        @Override
        public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
            return flatMapper.tryProcess(watermark);
        }
    }

    /**
     * A source processor that saves dummy constant data to the snapshot and
     * asserts that it receives the same data. It emits no output and never
     * completes.
     */
    public static class DummyStatefulP extends AbstractProcessor {
        public static volatile boolean wasRestored;
        public static int parallelism;
        private static final int ITEMS_TO_SAVE = 100;

        private Traverser<Map.Entry<BroadcastKey<Integer>, Integer>> traverser;
        private int[] restored;

        @Override
        public boolean complete() {
            return false;
        }

        @Override
        public boolean saveToSnapshot() {
            if (traverser == null) {
                traverser = traverseStream(IntStream.range(0, ITEMS_TO_SAVE)
                                                    .mapToObj(i -> entry(broadcastKey(i), i)))
                        .onFirstNull(() -> traverser = null);
            }
            return emitFromTraverserToSnapshot(traverser);
        }

        @Override
        protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
            if (restored == null) {
                restored = new int[ITEMS_TO_SAVE];
            }
            restored[(Integer) value]++;
        }

        @Override
        public boolean finishSnapshotRestore() {
            assertEquals(IntStream.generate(() -> parallelism).limit(ITEMS_TO_SAVE).boxed().collect(toList()),
                    IntStream.of(restored).boxed().collect(toList()));
            restored = null;
            wasRestored = true;
            return true;
        }
    }

    /**
     * A source processors that takes a collection of lists, one for each
     * processor. Each processor instance then emits one of the lists.
     */
    public static final class ListsSourceP implements ProcessorSupplier {

        private List<?>[] lists;

        ListsSourceP(List<?>... lists) {
            this.lists = lists;
        }

        @Override
        public void init(@Nonnull Context context) {
            if (context.totalParallelism() != lists.length) {
                throw new IllegalArgumentException("Supplied list count is not equal to total parallelism");
            }
            int fromIndex = context.memberIndex() * context.localParallelism();
            // We overwrite the field, but at this moment the processor supplier instance is cloned for
            // each member.
            lists = Arrays.copyOfRange(lists, fromIndex, fromIndex + context.localParallelism());
        }

        @Nonnull @Override
        public Collection<? extends Processor> get(int count) {
            assertEquals(lists.length, count);
            return Arrays.stream(lists).map(ListSource::new).collect(
                    Collectors.toList());
        }
    }

    /**
     * A processor that adds received items to a List, there's a separate list
     * per processor. The test can examine each of these lists independently
     * and check what was delivered to each processor.
     */
    public static final class CollectPerProcessorSink implements ProcessorMetaSupplier {

        static List<Address> members;
        static List<List<Object>> lists;

        List<Object> getListAt(int i) {
            return lists.get(i);
        }

        List<List<Object>> getLists() {
            return lists;
        }

        @Override
        public void init(@Nonnull Context context) {
            lists = IntStream.range(0, context.totalParallelism()).mapToObj(i -> new ArrayList<>()).collect(toList());
            members = new ArrayList<>(context.memberCount());
            for (int i = 0; i < context.memberCount(); i++) {
                // add placeholders for the members
                members.add(null);
            }
        }

        @Nonnull @Override
        public Function<? super Address, ? extends ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return address -> ProcessorSupplier.of(() -> new AbstractProcessor() {
                private List<Object> list;

                @Override
                protected void init(@Nonnull Context context) {
                    this.list = lists.get(context.globalProcessorIndex());
                    members.set(context.memberIndex(), context.hazelcastInstance().getCluster().getLocalMember().getAddress());
                }

                @Override
                protected boolean tryProcess(int ordinal, @Nonnull Object item) {
                    return list.add(item);
                }
            });
        }

        public List<Address> getMembers() {
            return members;
        }
    }
}
