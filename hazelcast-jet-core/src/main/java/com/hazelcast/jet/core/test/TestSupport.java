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

package com.hazelcast.jet.core.test;

import com.hazelcast.config.NetworkConfig;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Processor.Context;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingServiceImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.util.concurrent.IdleStrategy;

import javax.annotation.Nonnull;
import java.net.UnknownHostException;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BiPredicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.core.test.JetAssert.assertEquals;
import static com.hazelcast.jet.core.test.JetAssert.assertTrue;
import static com.hazelcast.jet.function.DistributedFunction.identity;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toMap;

/**
 * A utility to test processors. It will initialize the processor instance,
 * pass input items to it and assert the outbox contents.
 * <p>
 * The test process does the following:
 * <ul>
 *     <li>initializes the processor by calling {@link Processor#init}
 *
 *     <li>does snapshot or snapshot+restore (optional, see below)
 *
 *     <li>calls {@link Processor#process}, in two scenarios:<ul>
 *         <li>the inbox contains one input item</li>
 *         <li>the inbox contains all input items (if snapshots are not restored)</li>
 *     </ul>
 *
 *     <li>every time the inbox gets empty does snapshot or snapshot+restore
 *
 *     <li>{@link #disableCompleteCall() optionally} calls {@link Processor#complete()}
 *     until it returns {@code true} or calls it until {@link #disableRunUntilCompleted(long)
 *     specified timeout} elapses (for streaming sources)
 *
 *     <li>does snapshot or snapshot+restore each time the {@code complete()}
 *     method returned {@code false} and made a progress
 * </ul>
 *
 * The {@code init()} and {@code complete()} methods of {@link
 * ProcessorSupplier} and {@link ProcessorMetaSupplier} are called if you call
 * the {@link #verifyProcessor} using one of these.
 *
 * <h3>Snapshot &amp; restore</h3>
 * The {@link #disableSnapshots() optional} snapshot+restore test procedure:
 * <ul>
 *     <li>{@code saveToSnapshot()} is called. If we are not doing restore, this
 *     is the last step.
 *
 *     <li>new processor instance is created, from now on only this
 *     instance will be used
 *
 *     <li>snapshot is restored using {@code restoreFromSnapshot()}
 *
 *     <li>{@code finishSnapshotRestore()} is called
 * </ul>
 *
 * <h3>Watermark handling</h3>
 * The input can contain {@link Watermark}s. They will be passed to the
 * {@link Processor#tryProcessWatermark} method and can be asserted in the
 * output.
 *
 * <h3>Progress assertion</h3>
 * For each call to any processing method the progress is asserted ({@link
 * #disableProgressAssertion() optional}). The processor must do at least one
 * of these:
 * <ul>
 *     <li>take something from inbox
 *     <li>put something to outbox
 *     <li>for boolean-returning methods, returning {@code true} is
 *     considered as making progress
 * </ul>
 *
 * <h3>Outbox rejection</h3>
 * A 1-capacity outbox will be provided, which will additionally be full in
 * every other call to {@code process()}. This will test the edge case: the
 * {@code process()} method is called even when the outbox is full to give
 * the processor a chance to process the inbox. The snapshot bucket will
 * also have capacity of 1.
 *
 * <h3>Cooperative processors</h3>
 * For cooperative processors, time spent in each call to processing method
 * must not exceed {@link #cooperativeTimeout(long)}.
 *
 * <h3>Non-covered cases</h3>
 * This class does not cover these cases:
 * <ul>
 *     <li>Checking that the state of a stateful processor is empty at the
 *     end (you can do that yourself afterwards with the last instance
 *     returned from your supplier).
 *     <li>This utility never calls {@link Processor#tryProcess()}.
 * </ul>
 * <p/>
 *
 * <h3>Example usage</h3>
 * This will test one of the jet-provided processors:
 * <pre>{@code
 * TestSupport.verifyProcessor(Processors.map((String s) -> s.toUpperCase()))
 *            .disableCompleteCall()             // enabled by default
 *            .disableLogging()                  // enabled by default
 *            .disableProgressAssertion()        // enabled by default
 *            .disableSnapshots()                // enabled by default
 *            .cooperativeTimeout(<timeoutInMs>) // default is 1000
 *            .outputChecker(<function>)         // default is `Objects::equal`
 *            .input(asList("foo", "bar"))       // default is `emptyList()`
 *            .expectOutput(asList("FOO", "BAR"));
 * }</pre>
 */
public final class TestSupport {

    /**
     * An output checker that will claim actual and expected object lists as
     * equal if they both contain the same items, in any order. If some item is
     * expected multiple times, it must also be present the same number of
     * times in the actual output.
     * <p>
     * Use as an argument for {@link #outputChecker(BiPredicate)}.
     */
    public static final BiPredicate<List<?>, List<?>> SAME_ITEMS_ANY_ORDER =
            (expected, actual) -> {
                if (expected.size() != actual.size()) { // shortcut
                    return false;
                }
                Map<Object, Integer> expectedMap = expected.stream().collect(toMap(identity(), e -> 1, Integer::sum));
                Map<Object, Integer> actualMap = actual.stream().collect(toMap(identity(), e -> 1, Integer::sum));
                return expectedMap.equals(actualMap);
            };

    private static final Address LOCAL_ADDRESS;

    // 1ms should be enough for a cooperative call. We warn, when it's more than 5ms and
    // fail when it's more than 100ms, possibly due to other activity in the system, such as
    // tests running in parallel or a GC.
    private static final long COOPERATIVE_TIME_LIMIT_MS_FAIL = 5_000;
    private static final long COOPERATIVE_TIME_LIMIT_MS_WARN = 5;

    private static final long BLOCKING_TIME_LIMIT_MS_WARN = 10000;

    private static final LoggingServiceImpl LOGGING_SERVICE = new LoggingServiceImpl(
            "test-group", null, BuildInfoProvider.getBuildInfo()
    );

    static {
        try {
            LOCAL_ADDRESS = new Address("localhost", NetworkConfig.DEFAULT_PORT);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    private ProcessorMetaSupplier metaSupplier;
    private ProcessorSupplier supplier;
    private List<List<?>> inputs = emptyList();
    private List<List<?>> expectedOutputs = emptyList();
    private int[] priorities = {};
    private boolean assertProgress = true;
    private boolean doSnapshots = true;
    private boolean logInputOutput = true;
    private boolean callComplete = true;
    private JetInstance jetInstance;
    private long cooperativeTimeout = COOPERATIVE_TIME_LIMIT_MS_FAIL;
    private long runUntilCompletedTimeout;

    private BiPredicate<? super List<?>, ? super List<?>> outputChecker = Objects::equals;

    private TestSupport(@Nonnull ProcessorMetaSupplier metaSupplier) {
        this.metaSupplier = metaSupplier;
    }

    /**
     * @param supplier a processor supplier create processor instances
     */
    public static TestSupport verifyProcessor(@Nonnull DistributedSupplier<Processor> supplier) {
        return new TestSupport(ProcessorMetaSupplier.of(supplier));
    }

    /**
     * @param supplier a processor supplier create processor instances
     */
    public static TestSupport verifyProcessor(@Nonnull ProcessorSupplier supplier) {
        return new TestSupport(ProcessorMetaSupplier.of(supplier));
    }

    /**
     * @param supplier a processor supplier create processor instances
     */
    public static TestSupport verifyProcessor(@Nonnull ProcessorMetaSupplier supplier) {
        return new TestSupport(supplier);
    }

    /**
     * Sets the input objects for processor.
     * <p>
     * The {@code input} can contain {@link Watermark}s;
     * they will be delivered to the {@link Processor#tryProcessWatermark}
     * method.
     * <p>
     * Defaults to empty list.
     *
     * @return {@code this} instance for fluent API
     */
    public TestSupport input(@Nonnull List<?> input) {
        this.inputs = singletonList(input);
        this.priorities = new int[]{0};
        return this;
    }

    /**
     * Sets the input objects for the processor on multiple input ordinals.
     * Items will be passed to the processor in round-robin fashion:
     * item0 from input0, item0 from input1, item1 from input0 etc.
     * <p>
     * See also:<ul>
     *     <li>{@link #input(List)} - if you have just one input ordinal
     *     <li>{@link #inputs(List, int[])} - if you want to specify input
     *     priorities
     * </ul>
     *
     * @param inputs one list of input items for each input edge
     * @return {@code this} instance for fluent API
     */
    public TestSupport inputs(@Nonnull List<List<?>> inputs) {
        return inputs(inputs, new int[inputs.size()]);
    }

    /**
     * Sets the input objects for the processor on multiple input ordinals.
     * Items will be passed to the processor according to priority: lower
     * is higher. If two inputs have the same priority, they will be passed in
     * round-robin fashion.
     * <p>
     * See also:<ul>
     *     <li>{@link #input(List)} - if you have just one input ordinal
     *     <li>{@link #inputs(List)} - if all inputs are of equal priority
     * </ul>
     *
     * @param inputs one list of input items for each input edge
     * @return {@code this} instance for fluent API
     */
    public TestSupport inputs(@Nonnull List<List<?>> inputs, int[] priorities) {
        if (inputs.size() != priorities.length) {
            throw new IllegalArgumentException("Number of inputs must be equal to number of priorities");
        }
        this.inputs = inputs;
        this.priorities = priorities;
        return this;
    }

    /**
     * Sets the expected output and runs the test.
     * <p>
     * The {@code expectedOutput} can contain {@link
     * com.hazelcast.jet.core.Watermark}s. Each Watermark in the input will be
     * found in the output, as well as other watermarks the processor emits.
     *
     * @throws AssertionError If some assertion does not hold.
     */
    public void expectOutput(@Nonnull List<?> expectedOutput) {
        expectOutputs(singletonList(expectedOutput));
    }

    /**
     * Sets the expected outputs and runs the test.
     * <p>
     * The {@code expectedOutput} can contain {@link
     * com.hazelcast.jet.core.Watermark}s. Each Watermark in the input will be
     * found in the output, as well as other watermarks the processor emits.
     *
     * @param expectedOutputs one list for each out output ordinal
     * @throws AssertionError if some assertion does not hold
     */
    public void expectOutputs(@Nonnull List<List<?>> expectedOutputs) {
        try {
            TestProcessorMetaSupplierContext metaSupplierContext = new TestProcessorMetaSupplierContext();
            if (jetInstance != null) {
                metaSupplierContext.setJetInstance(jetInstance);
            }
            metaSupplier.init(metaSupplierContext);
            supplier = metaSupplier.get(singletonList(LOCAL_ADDRESS)).apply(LOCAL_ADDRESS);
            TestProcessorSupplierContext supplierContext = new TestProcessorSupplierContext();
            if (jetInstance != null) {
                supplierContext.setJetInstance(jetInstance);
            }
            supplier.init(supplierContext);
            this.expectedOutputs = expectedOutputs;
            runTest(false, 0, 1);
            if (inputs.stream().mapToInt(List::size).sum() > 0) {
                // only run this version if there is an input
                runTest(false, 0, Integer.MAX_VALUE);
            }
            if (doSnapshots) {
                runTest(true, 1, 1);
                runTest(true, 2, 1);
                runTest(true, Integer.MAX_VALUE, 1);
            }
            supplier.close(null);
        } catch (Exception e) {
            throw sneakyThrow(e);
        }
    }

    /**
     * Disables checking of progress of processing methods (see {@link
     * TestSupport class javadoc} for information on what is "progress").
     *
     * @return {@code this} instance for fluent API
     */
    public TestSupport disableProgressAssertion() {
        this.assertProgress = false;
        return this;
    }

    /**
     * Normally, the {@code complete()} method is run repeatedly until it
     * returns {@code true}. But in infinite source processors the method never
     * returns {@code true}. To be able test such processors, this method
     * allows you to disable the "run until completed" behavior and instead run
     * the {@code complete()} for a specified time.
     * <p>
     * If the timeout > 0, the {@code complete()} method is called repeatedly
     * until the timeout elapses. After that, the output is compared using the
     * {@link #outputChecker(BiPredicate) output checker}. The {@code
     * complete()} method is also not allowed to return {@code true} in this
     * case.
     * <p>
     * If the timeout is <= 0 (the default), {@code complete()} method is
     * called until it returns {@code true}, after which the output is checked.
     * <p>
     * Has no effect if {@code complete()} call is {@link #disableCompleteCall()
     * disabled}.
     *
     * @param timeoutMillis how long to wait until outputs match
     * @return {@code this} instance for fluent API
     */
    public TestSupport disableRunUntilCompleted(long timeoutMillis) {
        this.runUntilCompletedTimeout = timeoutMillis;
        return this;
    }

    /**
     * Disable snapshot save and restore before first item and after each
     * {@code process()} and {@code complete()} call.
     *
     * @return {@code this} instance for fluent API
     */
    public TestSupport disableSnapshots() {
        this.doSnapshots = false;
        return this;
    }

    /**
     * Disables logging of input and output objects. Normally they are logged
     * as they are processed to standard output.
     *
     * @return {@code this} instance for fluent API
     */
    public TestSupport disableLogging() {
        this.logInputOutput = false;
        return this;
    }

    /**
     * Disables calling {@code complete()} method during the test. Suitable for
     * testing of streaming processors to make sure that the flushing code in
     * {@code complete()} method is not executed.
     *
     * @return {@code this} instance for fluent API
     */
    public TestSupport disableCompleteCall() {
        this.callComplete = false;
        return this;
    }

    /**
     * If {@code timeout > 0}, the test will fail if any call to processing
     * method in a cooperative processor exceeds this timeout. Has no effect
     * for non-cooperative processors.
     * <p>
     * Default value is {@link #COOPERATIVE_TIME_LIMIT_MS_FAIL} ms. Useful to
     * set to 0 during debugging.
     *
     * @return {@code this} instance for fluent API
     */
    public TestSupport cooperativeTimeout(long timeout) {
        this.cooperativeTimeout = timeout;
        return this;
    }

    /**
     * Predicate to compare expected and actual output. Parameters to the
     * {@code BiPredicate} are the list of expected items and the list of actual
     * processor output.
     * <p>
     * Defaults to {@code Objects::equals}, which will pass, if both lists
     * contain equal objects in the same order. If the ordering doesn't matter,
     * you can use {@link #SAME_ITEMS_ANY_ORDER}.
     *
     * @return {@code this} instance for fluent API
     */
    public TestSupport outputChecker(@Nonnull BiPredicate<? super List<?>, ? super List<?>> outputChecker) {
        this.outputChecker = outputChecker;
        return this;
    }

    /**
     * Use the given instance for {@link Context#jetInstance()}
     *
     * @return {@code this} instance for fluent API
     */
    public TestSupport jetInstance(@Nonnull JetInstance jetInstance) {
        this.jetInstance = jetInstance;
        return this;
    }

    private static String modeDescription(boolean doSnapshots, int doRestoreEvery, int inboxLimit) {
        String sInboxSize = inboxLimit == Integer.MAX_VALUE ? "unlimited" : String.valueOf(inboxLimit);
        if (!doSnapshots && doRestoreEvery == 0) {
            return "snapshots disabled, inboxLimit=" + sInboxSize;
        } else if (doSnapshots && doRestoreEvery == 1) {
            assert inboxLimit == 1;
            return "snapshots enabled, restoring every snapshot";
        } else if (doSnapshots && doRestoreEvery == 2) {
            assert inboxLimit == 1;
            return "snapshots enabled, restoring every other snapshot";
        } else if (doSnapshots && doRestoreEvery == Integer.MAX_VALUE) {
            return "snapshots enabled, never restoring them, inboxLimit=" + sInboxSize;
        } else {
            throw new IllegalArgumentException("Unknown mode, doSnapshots=" + doSnapshots + ", doRestoreEvery="
                    + doRestoreEvery);
        }
    }

    private void runTest(boolean doSnapshots, int doRestoreEvery, int inboxLimit) throws Exception {
        assert doSnapshots || doRestoreEvery == 0 : "Illegal combination: don't do snapshots, but do restore";
        IdleStrategy idler = new BackoffIdleStrategy(0, 0, MICROSECONDS.toNanos(1),
                MILLISECONDS.toNanos(1));
        int idleCount = 0;
        System.out.println("### Running the test, mode=" + modeDescription(doSnapshots, doRestoreEvery, inboxLimit));

        TestInbox inbox = new TestInbox();
        int inboxOrdinal = -1;
        Processor[] processor = {newProcessorFromSupplier()};
        boolean isCooperative = processor[0].isCooperative();

        // we'll use 1-capacity outbox to test outbox rejection
        TestOutbox[] outbox = {createOutbox()};
        List<List<Object>> actualOutputs = new ArrayList<>(expectedOutputs.size());
        for (int i = 0; i < expectedOutputs.size(); i++) {
            actualOutputs.add(new ArrayList());
        }

        // create instance of your processor and call the init() method
        initProcessor(processor[0], outbox[0]);

        int[] restoreCount = {0};

        // do snapshot+restore before processing any item. This will test saveToSnapshot() in this edge case
        snapshotAndRestore(processor, outbox, actualOutputs, doSnapshots, doRestoreEvery, restoreCount);

        // call the process() method
        List<ObjectWithOrdinal> input = mixInputs(inputs, priorities);
        int inputPosition = 0;
        Watermark[] wmToProcess = {null};
        while (inputPosition < input.size() || !inbox.isEmpty() || wmToProcess[0] != null) {
            if (inbox.isEmpty() && wmToProcess[0] == null && inputPosition < input.size()) {
                inboxOrdinal = input.get(inputPosition).ordinal;
                for (int added = 0;
                        inputPosition < input.size()
                                && added < inboxLimit
                                && inboxOrdinal == input.get(inputPosition).ordinal
                                && (added == 0 || !(input.get(inputPosition).item instanceof Watermark));
                        added++
                ) {
                    ObjectWithOrdinal objectWithOrdinal = input.get(inputPosition++);
                    inbox.queue().add(objectWithOrdinal.item);
                    inboxOrdinal = objectWithOrdinal.ordinal;
                }
                if (logInputOutput) {
                    System.out.println(LocalTime.now() + " Input-" + inboxOrdinal + ": " + inbox);
                }
            }
            int lastInboxSize = inbox.size();
            String methodName;
            if (wmToProcess[0] != null) {
                methodName = "offer";
                if (outbox[0].offer(wmToProcess[0])) {
                    wmToProcess[0] = null;
                }
            } else {
                methodName = processInbox(inbox, inboxOrdinal, isCooperative, processor, wmToProcess);
            }
            boolean madeProgress = inbox.size() < lastInboxSize || !outbox[0].queue(0).isEmpty();
            assertTrue(methodName + "() call without progress", !assertProgress || madeProgress);
            idleCount = idle(idler, idleCount, madeProgress);
            if (outbox[0].queue(0).size() == 1 && !inbox.isEmpty()) {
                // if the outbox is full, call the process() method again. Cooperative
                // processor must be able to cope with this situation and not try to put
                // more items to the outbox.
                outbox[0].reset();
                processInbox(inbox, inboxOrdinal, isCooperative, processor, wmToProcess);
            }
            outbox[0].drainQueuesAndReset(actualOutputs, logInputOutput);
            if (inbox.isEmpty() && wmToProcess[0] == null) {
                snapshotAndRestore(processor, outbox, actualOutputs, doSnapshots, doRestoreEvery, restoreCount);
            }
        }

        if (logInputOutput && !inputs.isEmpty()) {
            System.out.println(LocalTime.now() + " Input processed, calling complete()");
        }

        // call the complete() method
        if (callComplete) {
            long completeStart = System.nanoTime();
            boolean[] done = {false};
            double elapsed;
            do {
                checkTime("complete", isCooperative, () -> done[0] = processor[0].complete());
                boolean madeProgress = done[0] || !outbox[0].queue(0).isEmpty();
                assertTrue("complete() call without progress", !assertProgress || madeProgress);
                outbox[0].drainQueuesAndReset(actualOutputs, logInputOutput);
                snapshotAndRestore(processor, outbox, actualOutputs, madeProgress && doSnapshots && !done[0],
                        doRestoreEvery, restoreCount);
                idleCount = idle(idler, idleCount, madeProgress);
                if (runUntilCompletedTimeout > 0) {
                    elapsed = toMillis(System.nanoTime() - completeStart);
                    if (elapsed > runUntilCompletedTimeout) {
                        break;
                    }
                }
            } while (!done[0]);
            assertTrue("complete returned true", !done[0] || runUntilCompletedTimeout <= 0);
        }

        processor[0].close();

        // assert the outbox
        for (int i = 0; i < expectedOutputs.size(); i++) {
            List<?> expectedOutput = expectedOutputs.get(i);
            List<?> actualOutput = actualOutputs.get(i);
            if (!outputChecker.test(expectedOutput, actualOutput)) {
                assertEquals("processor output in mode \"" + modeDescription(doSnapshots, doRestoreEvery, inboxLimit)
                        + "\" doesn't match", listToString(expectedOutput), listToString(actualOutput));
            }
        }
    }

    private Processor newProcessorFromSupplier() {
        return supplier.get(1).iterator().next();
    }

    /**
     * Sorts the objects from multiple inputs into an order in which they will
     * be passed to processor, based on priorities.
     */
    private static List<ObjectWithOrdinal> mixInputs(List<List<?>> inputs, int[] priorities) {
        SortedMap<Integer, List<Integer>> ordinalsByPriority = new TreeMap<>();
        for (int i = 0; i < priorities.length; i++) {
            ordinalsByPriority.computeIfAbsent(priorities[i], k -> new ArrayList<>())
                            .add(i);
        }

        List<ObjectWithOrdinal> result = new ArrayList<>();
        for (List<Integer> ordinals : ordinalsByPriority.values()) {
            boolean allDone;
            int index = 0;
            do {
                allDone = true;
                for (Integer ordinal : ordinals) {
                    if (inputs.get(ordinal).size() > index) {
                        Object item = inputs.get(ordinal).get(index);
                        result.add(new ObjectWithOrdinal(ordinal, item));
                        allDone = false;
                    }
                }
                index++;
            } while (!allDone);
        }

        return result;
    }

    private TestOutbox createOutbox() {
        return new TestOutbox(IntStream.generate(() -> 1).limit(expectedOutputs.size()).toArray(), 1);
    }

    private String processInbox(TestInbox inbox, int inboxOrdinal, boolean isCooperative, Processor[] processor,
                                Watermark[] wmToEmit) {
        if (inbox.peek() instanceof Watermark) {
            Watermark wm = ((Watermark) inbox.peek());
            checkTime("tryProcessWatermark", isCooperative, () -> {
                if (processor[0].tryProcessWatermark(wm)) {
                    inbox.remove();
                    wmToEmit[0] = wm;
                }
            });
            return "tryProcessWatermark";
        } else {
            checkTime("process", isCooperative, () -> processor[0].process(inboxOrdinal, inbox));
            return "process";
        }
    }

    private int idle(IdleStrategy idler, int idleCount, boolean madeProgress) {
        if (!madeProgress) {
            idler.idle(++idleCount);
        } else {
            idleCount = 0;
        }
        return idleCount;
    }

    private void snapshotAndRestore(
            Processor[] processor,
            TestOutbox[] outbox,
            List<List<Object>> actualOutput,
            boolean doSnapshot,
            int doRestoreEvery,
            int[] restoreCount) throws Exception {
        if (!doSnapshot) {
            return;
        }

        restoreCount[0]++;
        boolean willRestore = restoreCount[0] % doRestoreEvery == 0;
        if (logInputOutput) {
            System.out.println(LocalTime.now() + (willRestore
                    ? " Saving & restoring snapshot"
                    : " Saving snapshot without restoring it"));
        }

        // save state of current processor
        TestInbox snapshotInbox = new TestInbox();
        boolean[] done = {false};
        boolean isCooperative = processor[0].isCooperative();
        Set<Object> keys = new HashSet<>();
        do {
            checkTime("saveSnapshot", isCooperative, () -> done[0] = processor[0].saveToSnapshot());
            assertTrue("saveToSnapshot() call without progress",
                    !assertProgress || done[0] || !outbox[0].snapshotQueue().isEmpty()
                            || !outbox[0].queue(0).isEmpty());
            outbox[0].drainSnapshotQueueAndReset(snapshotInbox.queue(), false);
            outbox[0].drainQueuesAndReset(actualOutput, logInputOutput);
        } while (!done[0]);

        // check snapshot for duplicate keys
        for (Object item : snapshotInbox.queue()) {
            Entry<Object, Object> item2 = (Entry<Object, Object>) item;
            assertTrue("Duplicate key produced in saveToSnapshot()\n  " +
                    "Duplicate: " + item2.getKey() + "\n  Keys so far: " + keys, keys.add(item2.getKey()));
        }

        if (!willRestore) {
            return;
        }

        // restore state to new processor
        assert outbox[0].queue(0).isEmpty();
        assert outbox[0].snapshotQueue().isEmpty();
        processor[0].close();
        processor[0] = newProcessorFromSupplier();
        outbox[0] = createOutbox();
        initProcessor(processor[0], outbox[0]);

        int lastInboxSize = snapshotInbox.queue().size();
        while (!snapshotInbox.isEmpty()) {
            checkTime("restoreSnapshot", isCooperative,
                    () -> processor[0].restoreFromSnapshot(snapshotInbox));
            assertTrue("restoreFromSnapshot() call without progress",
                    !assertProgress
                            || lastInboxSize > snapshotInbox.queue().size()
                            || !outbox[0].queue(0).isEmpty());
            outbox[0].drainQueuesAndReset(actualOutput, logInputOutput);
            lastInboxSize = snapshotInbox.queue().size();
        }
        do {
            checkTime("finishSnapshotRestore", isCooperative,
                    () -> done[0] = processor[0].finishSnapshotRestore());
            assertTrue("finishSnapshotRestore() call without progress",
                    !assertProgress || done[0] || !outbox[0].queue(0).isEmpty());
            outbox[0].drainQueuesAndReset(actualOutput, logInputOutput);
        } while (!done[0]);
    }

    private void checkTime(String methodName, boolean isCooperative, Runnable r) {
        long start = System.nanoTime();
        r.run();
        long elapsed = System.nanoTime() - start;

        if (isCooperative) {
            if (cooperativeTimeout > 0) {
                assertTrue(String.format("call to %s() took %.1fms, it should be <%dms", methodName,
                        toMillis(elapsed), COOPERATIVE_TIME_LIMIT_MS_FAIL),
                        elapsed < MILLISECONDS.toNanos(COOPERATIVE_TIME_LIMIT_MS_FAIL));
            }
            // print warning
            if (elapsed > MILLISECONDS.toNanos(COOPERATIVE_TIME_LIMIT_MS_WARN)) {
                System.out.println(String.format("Warning: call to %s() took %.2fms, it should be <%dms normally",
                        methodName, toMillis(elapsed), COOPERATIVE_TIME_LIMIT_MS_WARN));
            }
        } else {
            if (elapsed > MILLISECONDS.toNanos(BLOCKING_TIME_LIMIT_MS_WARN)) {
                System.out.println(String.format("Warning: call to %s() took %.2fms in non-cooperative processor. Is " +
                                "this expected?", methodName, toMillis(elapsed)));
            }
        }
    }

    private void initProcessor(Processor processor, TestOutbox outbox) {
        TestProcessorContext context = new TestProcessorContext()
                .setLogger(getLogger(processor.getClass().getName()));
        if (jetInstance != null) {
            context.setJetInstance(jetInstance);
        }
        try {
            processor.init(outbox, context);
        } catch (Exception e) {
            throw sneakyThrow(e);
        }
    }

    private static double toMillis(long nanos) {
        return nanos / (double) MILLISECONDS.toNanos(1);
    }

    /**
     * Wraps the provided {@code ProcessorSupplier} with a {@code
     * Supplier<Processor>} that returns processors obtained from it.
     */
    public static Supplier<Processor> supplierFrom(ProcessorSupplier supplier) {
        return supplierFrom(supplier, new TestProcessorSupplierContext());
    }

    /**
     * Wraps the provided {@code ProcessorSupplier} with a {@code
     * Supplier<Processor>} that returns processors obtained from it.
     */
    public static Supplier<Processor> supplierFrom(ProcessorSupplier supplier, ProcessorSupplier.Context context) {
        try {
            supplier.init(context);
        } catch (Exception e) {
            throw sneakyThrow(e);
        }
        return () -> supplier.get(1).iterator().next();
    }

    /**
     * Wraps the provided {@code ProcessorMetaSupplier} with a {@code
     * Supplier<Processor>} that returns processors obtained from it.
     */
    public static Supplier<Processor> supplierFrom(ProcessorMetaSupplier supplier) {
        return supplierFrom(supplier, new TestProcessorSupplierContext());
    }

    /**
     * Wraps the provided {@code ProcessorMetaSupplier} with a {@code
     * Supplier<Processor>} that returns processors obtained from it.
     */
    public static Supplier<Processor> supplierFrom(ProcessorMetaSupplier supplier,
                                                   ProcessorSupplier.Context context) {
        try {
            supplier.init(context);
        } catch (Exception e) {
            throw sneakyThrow(e);
        }
        return supplierFrom(supplier.get(singletonList(LOCAL_ADDRESS)).apply(LOCAL_ADDRESS), context);
    }

    static ILogger getLogger(String name) {
        return LOGGING_SERVICE.getLogger(name);
    }

    static ILogger getLogger(Class clazz) {
        return LOGGING_SERVICE.getLogger(clazz);
    }

    /**
     * Converts a list to a string putting {@code toString()} of each element
     * on separate line. It is useful to transform list inputs to {@code
     * assertEquals()}: the exception will show the entire collections instead
     * of just non-equal sizes or the first non-equal element.
     *
     * @param list Input list
     * @return Output string
     */
    public static String listToString(List<?> list) {
        return list.stream()
                   .map(String::valueOf)
                   .collect(Collectors.joining("\n"));
    }

    private static class ObjectWithOrdinal {
        final int ordinal;
        final Object item;

        ObjectWithOrdinal(int ordinal, Object item) {
            this.ordinal = ordinal;
            this.item = item;
        }
    }
}
