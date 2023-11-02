/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastTestSupport;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
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
import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.test.HazelcastTestSupport.sleepMillis;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public final class TestProcessors {

    private static final ILogger LOGGER = Logger.getLogger(HazelcastTestSupport.class);
    private static final Random RANDOM = new Random();
    private static final Set<String> errors = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private TestProcessors() { }

    /**
     * Reset the static counters in test processors. Call before starting each
     * test that uses them.
     */
    public static void reset(int totalParallelism) {
        errors.clear();
        MockPMS.initCount.set(0);
        MockPMS.closeCount.set(0);
        MockPMS.receivedCloseError.set(null);
        MockPMS.blockingSemaphore = new Semaphore(0, true);

        MockPS.nodeCount = -1;
        MockPS.closeCount.set(0);
        MockPS.initCount.set(0);
        MockPS.receivedCloseErrors.clear();
        MockPS.blockingSemaphore = new Semaphore(0, true);

        MockP.initCount.set(0);
        MockP.closeCount.set(0);
        MockP.saveToSnapshotCalled = false;
        MockP.onSnapshotCompletedCalled = false;
        MockP.blockingSemaphore = new Semaphore(0, true);

        NoOutputSourceP.proceedLatch = new CountDownLatch(1);
        NoOutputSourceP.executionStarted = new CountDownLatch(totalParallelism);
        NoOutputSourceP.initCount.set(0);
        NoOutputSourceP.failure.set(null);

        DummyStatefulP.parallelism = totalParallelism;
        DummyStatefulP.wasRestored = true;

        CollectPerProcessorSink.lists = null;
    }

    /**
     * Asserts that no errors were raised in processor's init and close methods.
     * Such errors normally are being "eaten" by the framework, so won't cause typical assertion error.
     * <p>
     * It checks also how many times init and close was called.
     */
    public static void assertNoErrorsInProcessors() {
        String errorString = String.join("\n", errors);
        assertTrue("There should be no errors in processors, but were: \n" + errorString, errors.isEmpty());
        MockPS.assertInitCloseCounts();
        MockPMS.assertInitCloseCounts();
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

    /**
     * If expression is false, given message will be added to set of errors and will cause
     * {@link TestProcessors#assertNoErrorsInProcessors()} to fail.
     */
    private static void assertTrueInProcessor(String message, boolean expression) {
        if (!expression) {
            errors.add(message);
        }
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

        static AtomicInteger initCount = new AtomicInteger();
        static AtomicInteger closeCount = new AtomicInteger();
        static AtomicReference<Throwable> receivedCloseError = new AtomicReference<>();
        static Semaphore blockingSemaphore = new Semaphore(0, true);

        private SupplierEx<Throwable> initError;
        private SupplierEx<Throwable> getError;
        private SupplierEx<Throwable> closeError;
        private volatile boolean initBlocks;
        private volatile boolean closeBlocks;

        private final SupplierEx<ProcessorSupplier> supplierFn;

        public MockPMS(SupplierEx<ProcessorSupplier> supplierFn) {
            this.supplierFn = supplierFn;
        }

        public MockPMS setInitError(SupplierEx<Throwable> initError) {
            this.initError = initError;
            return this;
        }

        public MockPMS setGetError(SupplierEx<Throwable> getError) {
            this.getError = getError;
            return this;
        }

        public MockPMS setCloseError(SupplierEx<Throwable> closeError) {
            this.closeError = closeError;
            return this;
        }

        public MockPMS initBlocks() {
            this.initBlocks = true;
            return this;
        }
        public MockPMS closeBlocks() {
            this.closeBlocks = true;
            return this;
        }

        public static void waitBlockingSemaphore() {
            while (blockingSemaphore.getQueueLength() > 0) {
                sleepMillis(1);
            }
        }
        public static void unblock() {
            blockingSemaphore.release();
        }

        @Override
        public boolean initIsCooperative() {
            return !initBlocks;
        }

        @Override
        public void init(@Nonnull Context context) throws InterruptedException {
            LOGGER.info("MockPMS.init called on " + Thread.currentThread().getName());
            initCount.incrementAndGet();

            if (initBlocks) {
                blockingSemaphore.acquire();
                Thread.sleep(RANDOM.nextInt(500));
            }

            if (initError != null) {
                throw sneakyThrow(initError.get());
            }
        }

        @Nonnull @Override
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            if (getError != null) {
                throw sneakyThrow(getError.get());
            }
            return a -> supplierFn.get();
        }

        @Override
        public boolean closeIsCooperative() {
            return !closeBlocks;
        }

        @Override
        public void close(Throwable error) throws InterruptedException {
            LOGGER.info("MockPMS.close called on " + Thread.currentThread().getName());
            if (closeBlocks) {
                blockingSemaphore.acquire();
                Thread.sleep(RANDOM.nextInt(500));
            }
            closeCount.incrementAndGet();
            assertTrueInProcessor("Close called without calling init()", initCount.get() != 0);
            assertTrueInProcessor("PMS#close() already called once",
                    receivedCloseError.compareAndSet(null, error)
            );

            if (closeError != null) {
                throw sneakyThrow(closeError.get());
            }
        }

        static void assertInitCloseCounts() {
            assertEquals("PMS#close called different number of times than init. Init count: "
                    + initCount.get() + " close count: " + closeCount, initCount.get(), closeCount.get());
        }

        static void assertsWhenOneJob() {
            assertEquals("PMS#close() should be called exactly once", 1, closeCount.get());
        }

        static void verifyCloseCount() {
            assertEquals("all PS that have been init should have been closed at this point",
                    MockPS.initCount.get(), MockPS.closeCount.get());
        }
    }

    public static class MockPS implements ProcessorSupplier {

        static AtomicInteger initCount = new AtomicInteger();
        static AtomicInteger closeCount = new AtomicInteger();
        static volatile int nodeCount;

        static List<Throwable> receivedCloseErrors = new CopyOnWriteArrayList<>();
        static Semaphore blockingSemaphore = new Semaphore(0, true);

        private SupplierEx<Throwable> initError;
        private SupplierEx<Throwable> getError;
        private SupplierEx<Throwable> closeError;

        private volatile boolean initBlocks;
        private volatile boolean closeBlocks;

        private final SupplierEx<Processor> supplier;
        private boolean initCalled;

        public MockPS(SupplierEx<Processor> supplier, int nodeCount) {
            this.supplier = supplier;
            MockPS.nodeCount = nodeCount;
        }

        public MockPS setInitError(SupplierEx<Throwable> initError) {
            this.initError = initError;
            return this;
        }

        public MockPS setGetError(SupplierEx<Throwable> getError) {
            this.getError = getError;
            return this;
        }

        public MockPS setCloseError(SupplierEx<Throwable> closeError) {
            this.closeError = closeError;
            return this;
        }

        public MockPS initBlocks() {
            this.initBlocks = true;
            return this;
        }

        public MockPS closeBlocks() {
            this.closeBlocks = true;
            return this;
        }

        public static void waitBlockingSemaphore() {
            while (blockingSemaphore.getQueueLength() > 0) {
                sleepMillis(1);
            }
        }

        public static void unblock() {
            blockingSemaphore.release();
        }

        @Override
        public boolean initIsCooperative() {
            return !initBlocks;
        }

        @Override
        public void init(@Nonnull Context context) throws InterruptedException {
            LOGGER.info("MockPS.init called on " + Thread.currentThread().getName());
            initCalled = true;
            initCount.incrementAndGet();


            if (initBlocks) {
                blockingSemaphore.acquire();
                Thread.sleep(RANDOM.nextInt(500));
            }

            if (initError != null) {
                throw sneakyThrow(initError.get());
            }

        }

        @Nonnull @Override
        public List<Processor> get(int count) {
            if (getError != null) {
                throw sneakyThrow(getError.get());
            }
            return Stream.generate(supplier).limit(count).collect(toList());
        }

        @Override
        public boolean closeIsCooperative() {
            return !closeBlocks;
        }

        @Override
        public void close(Throwable error) throws InterruptedException {
            String threadName = Thread.currentThread().getName();
            LOGGER.info("MockPS.close called on " + threadName);
            if (closeBlocks) {
                blockingSemaphore.acquire();
                Thread.sleep(RANDOM.nextInt(500));
                assertTrueInProcessor("executed not on offload thread, but: " + threadName, threadName.contains("cached.thread"));
            }
            if (error != null) {
                receivedCloseErrors.add(error);
            }
            closeCount.incrementAndGet();

            assertTrueInProcessor("PS#close called without calling PS#init()", initCalled);

            if (closeError != null) {
                throw sneakyThrow(closeError.get());
            }
        }

        public static void assertInitCloseCounts() {
            assertEquals("PS#close called " + closeCount.get() + " times, but PS#init called "
                    + initCount.get() + " times!", closeCount.get(), initCount.get());

            if (nodeCount != -1) {
                assertFalse("Close called without init being called on all the nodes. Init count: "
                        + initCount.get() + " node count: " + nodeCount, initCount.get() < nodeCount);
            }
        }
    }

    public static class MockP extends AbstractProcessor {

        static AtomicInteger initCount = new AtomicInteger();
        static AtomicInteger closeCount = new AtomicInteger();
        static volatile boolean onSnapshotCompletedCalled;
        static volatile boolean saveToSnapshotCalled;
        static Semaphore blockingSemaphore = new Semaphore(0, true);

        private SupplierEx<Throwable> initError;
        private SupplierEx<Throwable> processError;
        private SupplierEx<Throwable> completeError;
        private SupplierEx<Throwable> closeError;
        private SupplierEx<Throwable> onSnapshotCompleteError;
        private SupplierEx<Throwable> saveToSnapshotError;
        private boolean initBlocks;

        private boolean isCooperative;
        private boolean streaming;

        @Override
        public boolean isCooperative() {
            return isCooperative;
        }

        public MockP setInitError(SupplierEx<Throwable> initError) {
            this.initError = initError;
            return this;
        }

        public MockP setProcessError(SupplierEx<Throwable> processError) {
            this.processError = processError;
            return this;
        }

        public MockP setCompleteError(SupplierEx<Throwable> completeError) {
            this.completeError = completeError;
            return this;
        }

        public MockP setOnSnapshotCompleteError(SupplierEx<Throwable> e) {
            this.onSnapshotCompleteError = e;
            return this;
        }

        public MockP setSaveToSnapshotError(SupplierEx<Throwable> e) {
            this.saveToSnapshotError = e;
            return this;
        }

        public MockP setCloseError(SupplierEx<Throwable> closeError) {
            this.closeError = closeError;
            return this;
        }

        public MockP initBlocks() {
            this.initBlocks = true;
            return this;
        }

        public static void unblock() {
            blockingSemaphore.release();
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
        protected void init(@Nonnull Context context) throws InterruptedException {
            LOGGER.info("MockP.init called on " + Thread.currentThread().getName());
            initCount.incrementAndGet();

            // Block first to allow to control when the exception is thrown
            if (initBlocks) {
                blockingSemaphore.acquire();
                Thread.sleep(RANDOM.nextInt(500));
            }

            if (initError != null) {
                throw sneakyThrow(initError.get());
            }
        }

        @Override
        protected boolean tryProcess(int ordinal, @Nonnull Object item) {
            if (processError != null) {
                throw sneakyThrow(processError.get());
            }
            return tryEmit(item);
        }

        @Override
        public boolean complete() {
            if (completeError != null) {
                throw sneakyThrow(completeError.get());
            }
            return !streaming;
        }

        @Override
        public boolean saveToSnapshot() {
            saveToSnapshotCalled = true;
            if (saveToSnapshotError != null) {
                throw sneakyThrow(saveToSnapshotError.get());
            }
            return true;
        }

        @Override
        public boolean snapshotCommitFinish(boolean success) {
            onSnapshotCompletedCalled = true;
            if (onSnapshotCompleteError != null) {
                throw sneakyThrow(onSnapshotCompleteError.get());
            }
            return true;
        }

        @Override
        public void close() {
            LOGGER.info("MockP.close called on " + Thread.currentThread().getName());
            closeCount.incrementAndGet();
            if (closeError != null) {
                throw sneakyThrow(closeError.get());
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

        public List<Object> getListAt(int i) {
            return lists.get(i);
        }

        public List<List<Object>> getLists() {
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
