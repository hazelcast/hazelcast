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

package com.hazelcast.jet.core.test;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.SerializationServiceAware;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.internal.util.concurrent.IdleStrategy;
import com.hazelcast.jet.config.EdgeConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Processor.Context;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.impl.LoggingServiceImpl;
import com.hazelcast.spi.impl.SerializationServiceSupport;

import javax.annotation.Nonnull;
import java.net.UnknownHostException;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.function.FunctionEx.identity;
import static com.hazelcast.internal.util.Preconditions.checkNotNegative;
import static com.hazelcast.jet.core.test.JetAssert.assertEquals;
import static com.hazelcast.jet.core.test.JetAssert.assertFalse;
import static com.hazelcast.jet.core.test.JetAssert.assertTrue;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.Util.subtractClamped;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
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
 *     <li>every time the inbox gets empty it does snapshot or snapshot+restore
 *
 *     <li>{@linkplain #disableCompleteCall() optionally} calls {@link
 *     Processor#complete()} until it returns {@code true} or until the
 *     {@linkplain #runUntilOutputMatches output matches} (for streaming
 *     sources)
 *
 *     <li>does snapshot or snapshot+restore each time the {@code complete()}
 *     method returned {@code false} and made a progress
 * </ul>
 *
 * The {@code init()} and {@code close()} methods of {@link
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
 * {@link Processor#tryProcessWatermark} method.
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
 * TestSupport.verifyProcessor(Processors.map((String s) -> s.toUpperCase(Locale.ROOT)))
 *            .disableCompleteCall()             // enabled by default
 *            .disableLogging()                  // enabled by default
 *            .disableProgressAssertion()        // enabled by default
 *            .disableSnapshots()                // enabled by default
 *            .cooperativeTimeout(<timeoutInMs>) // default is 1000
 *            .outputChecker(<function>)         // default is `Objects::equal`
 *            .input(asList("foo", "bar"))       // default is `emptyList()`
 *            .expectOutput(asList("FOO", "BAR"));
 * }</pre>
 *
 * @since Jet 3.0
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
            "test-group", null, BuildInfoProvider.getBuildInfo(), true, null);

    static {
        try {
            LOCAL_ADDRESS = new Address("localhost", NetworkConfig.DEFAULT_PORT);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    private final ProcessorMetaSupplier metaSupplier;
    private ProcessorSupplier supplier;
    private List<ItemWithOrdinal> inputOutput = emptyList();
    private boolean assertProgress = true;
    private boolean doSnapshots = true;
    private boolean logInputOutput = true;
    private boolean callComplete = true;
    private int outputOrdinalCount;
    private boolean outputMustOccurOnTime;
    private final List<ItemWithOrdinal> accumulatedExpectedOutput = new ArrayList<>();
    private Runnable beforeEachRun = () -> { };

    private int localProcessorIndex;
    private int globalProcessorIndex;
    private int localParallelism = 1;
    private int totalParallelism = 1;

    private HazelcastInstance hazelcastInstance;
    private JobConfig jobConfig;
    private long cooperativeTimeout = COOPERATIVE_TIME_LIMIT_MS_FAIL;
    private long runUntilOutputMatchesTimeoutMillis = -1;
    private long runUntilOutputMatchesExtraTimeMillis;

    private BiConsumer<TestMode, List<List<Object>>> assertOutputFn;

    private BiPredicate<? super List<?>, ? super List<?>> outputChecker = Objects::equals;

    private TestSupport(@Nonnull ProcessorMetaSupplier metaSupplier) {
        this.metaSupplier = metaSupplier;
    }

    /**
     * @param supplier a processor supplier create processor instances
     */
    public static TestSupport verifyProcessor(@Nonnull SupplierEx<Processor> supplier) {
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
        this.inputOutput = mixInputs(singletonList(input), new int[]{0});
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
        this.inputOutput = mixInputs(inputs, priorities);
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
     * Specifies the expected outputs and runs the test.
     * <p>
     * The {@code expectedOutput} can contain {@link Watermark}s to assert the
     * watermarks emitted by the processor.
     *
     * @param expectedOutputs one list for each output ordinal
     * @throws AssertionError if some assertion does not hold
     */
    public void expectOutputs(@Nonnull List<List<?>> expectedOutputs) {
        if (!(inputOutput instanceof ArrayList)) {
            inputOutput = new ArrayList<>(inputOutput);
        }

        for (int ordinal = 0; ordinal < expectedOutputs.size(); ordinal++) {
            for (Object item : expectedOutputs.get(ordinal)) {
                inputOutput.add(out(ordinal, item));
            }
        }

        assertOutput(
                expectedOutputs.size(), (mode, actual) -> assertExpectedOutput(mode, expectedOutputs, actual)
        );
    }

    /**
     * Runs the test and expects an exact sequence of input and output items.
     * The output must occur in the expected order given by the {@code
     * inputOutput} parameter, that is a particular output item must occur after
     * particular input items. If the output happens at other time, the test
     * fails.
     * <p>
     * To create `ItemWithOrdinal` instances, use the {@link #in} and {@link
     * #out} static factory methods.
     * <p>
     * The output after the last input item is asserted after the `complete()`
     * method calls, not immediately after processing of the last input item.
     * <p>
     * The number of input and output edges of the processor will be equal to
     * the maximum ordinal found for input and output, plus one. If there's no
     * input or output item, the processor will have zero input or output
     * ordinals. Use a dummy {@code null} item if you want to increase the
     * number of ordinals in that case, this item will be ignored, except for
     * using its ordinal.
     *
     * @param inputOutput the input and expected output items
     */
    public void expectExactOutput(ItemWithOrdinal... inputOutput) {
        this.inputOutput = asList(inputOutput);

        outputOrdinalCount = this.inputOutput.stream()
                .filter(ItemWithOrdinal::isOutput)
                .mapToInt(ItemWithOrdinal::ordinal)
                .max()
                .orElse(-1) + 1;
        outputMustOccurOnTime = true;
        assertOutputFn = (mode, actual) -> assertExpectedOutput(mode, transformToListList(accumulatedExpectedOutput), actual);

        run();
    }

    /**
     * Runs the test with the specified custom assertion.
     * <p>
     * The consumer takes a list of collected output and the current test mode which
     * can be used in the assertion message.
     *
     * @param outputOrdinalCount how many output ordinals should be created
     * @param assertFn an assertion function which takes the current mode and the collected output
     */
    public void assertOutput(int outputOrdinalCount, BiConsumer<TestMode, List<List<Object>>> assertFn) {
        this.assertOutputFn = assertFn;

        this.outputOrdinalCount = outputOrdinalCount;
        outputMustOccurOnTime = false;

        run();
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
     * returns {@code true}. This is suitable for processors processing the
     * input or for batch sources. However, if you test a streaming source, the
     * {@code complete()} method never returns {@code true}. To be able to test
     * such processors, this method allows you to change the behavior to run
     * {@code complete()} until the output matches.
     * <p>
     * The {@code extraTimeMillis} parameter specifies an extra time to call
     * {@code complete()} after the output matches. It can be used to ensure
     * that no more items are produced after the output matches.
     * <p>
     * Has no effect if calling {@code complete()} is {@linkplain
     * #disableCompleteCall() disabled}.
     *
     * @param timeoutMillis maximum time to wait for the output to match
     * @param extraTimeMillis for how long to call {@code complete()}
     *                       after the output matches
     * @return {@code this} instance for fluent API
     */
    public TestSupport runUntilOutputMatches(long timeoutMillis, long extraTimeMillis) {
        checkNotNegative(timeoutMillis, "timeoutMillis must be >= 0");
        checkNotNegative(extraTimeMillis, "extraTimeMillis must be >= 0");
        this.runUntilOutputMatchesTimeoutMillis = timeoutMillis;
        this.runUntilOutputMatchesExtraTimeMillis = extraTimeMillis;
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
     * Sets the localProcessorIndex for the Processor
     *
     * @param localProcessorIndex localProcessorIndex, defaults to 0
     */
    public TestSupport localProcessorIndex(int localProcessorIndex) {
        this.localProcessorIndex = localProcessorIndex;
        return this;
    }

    /**
     * Sets the globalProcessorIndex for the Processor
     *
     * @param globalProcessorIndex globalProcessorIndex, default to 0
     */
    public TestSupport globalProcessorIndex(int globalProcessorIndex) {
        this.globalProcessorIndex = globalProcessorIndex;
        return this;
    }

    /**
     * Sets the localParallelism for the Processor
     *
     * @param localParallelism localParallelism, defaults to 1
     */
    public TestSupport localParallelism(int localParallelism) {
        this.localParallelism = localParallelism;
        return this;
    }

    /**
     * Sets the totalParallelism for the Processor
     *
     * @param totalParallelism totalParallelism, defaults to 1
     */
    public TestSupport totalParallelism(int totalParallelism) {
        this.totalParallelism = totalParallelism;
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
     * Use the given instance for {@link Context#hazelcastInstance()}
     *
     * @return {@code this} instance for fluent API
     */
    public TestSupport hazelcastInstance(@Nonnull HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
        return this;
    }

    /**
     * Use the given instance for {@link Context#jobConfig()}
     *
     * @return {@code this} instance for fluent API
     */
    public TestSupport jobConfig(JobConfig jobConfig) {
        this.jobConfig = jobConfig;
        return this;
    }

    /**
     * Action to execute before each test scenario.
     *
     * @return {@code this} instance for fluent API
     */
    public TestSupport executeBeforeEachRun(Runnable runnable) {
        this.beforeEachRun = runnable;
        return this;
    }

    private void run() {
        // filter null items from inputOutput. They are there only to affect the number of ordinals. See class javadoc.
        inputOutput.removeIf(item -> item.item == null);

        try {
            TestProcessorMetaSupplierContext metaSupplierContext = new TestProcessorMetaSupplierContext();
            if (hazelcastInstance != null) {
                metaSupplierContext.setHazelcastInstance(hazelcastInstance);
            }
            if (jobConfig != null) {
                metaSupplierContext.setJobConfig(jobConfig);
            }
            metaSupplier.init(metaSupplierContext);
            Address address = hazelcastInstance != null
                    ? hazelcastInstance.getCluster().getLocalMember().getAddress() : LOCAL_ADDRESS;
            supplier = metaSupplier.get(singletonList(address)).apply(address);
            TestProcessorSupplierContext supplierContext = new TestProcessorSupplierContext();
            if (hazelcastInstance != null) {
                supplierContext.setHazelcastInstance(hazelcastInstance);
            }
            if (jobConfig != null) {
                supplierContext.setJobConfig(jobConfig);
            }
            supplier.init(supplierContext);
            runTest(new TestMode(false, 0, 1));
            if (inputOutput.stream().anyMatch(ItemWithOrdinal::isInput)) {
                // only run this version if there is any input
                runTest(new TestMode(false, 0, EdgeConfig.DEFAULT_QUEUE_SIZE));
            }
            if (doSnapshots) {
                runTest(new TestMode(true, 1, 1));
                runTest(new TestMode(true, 2, 1));
                runTest(new TestMode(true, Integer.MAX_VALUE, 1));
            }
            supplier.close(null);
        } catch (Exception e) {
            throw sneakyThrow(e);
        }
    }

    @SuppressWarnings("checkstyle:BooleanExpressionComplexity")
    private void runTest(TestMode testMode) throws Exception {
        beforeEachRun.run();
        accumulatedExpectedOutput.clear();

        assert testMode.isSnapshotsEnabled() || testMode.snapshotRestoreInterval() == 0
            : "Illegal combination: don't do snapshots, but do restore";

        boolean doSnapshots = testMode.doSnapshots;
        int doRestoreEvery = testMode.restoreInterval;

        IdleStrategy idler = new BackoffIdleStrategy(0, 0, MICROSECONDS.toNanos(1),
                MILLISECONDS.toNanos(1));
        int idleCount = 0;
        System.out.println("### Running the test, mode=" + testMode);

        TestInbox inbox = new TestInbox();
        int inboxOrdinal = -1;
        Processor[] processor = {newProcessorFromSupplier()};
        boolean isCooperative = processor[0].isCooperative();

        // we'll use 1-capacity outbox to test outbox rejection
        TestOutbox[] outbox = {createOutbox()};
        List<List<Object>> actualOutputs = new ArrayList<>(outputOrdinalCount);
        for (int i = 0; i < outputOrdinalCount; i++) {
            actualOutputs.add(new ArrayList<>());
        }

        // create instance of your processor and call the init() method
        initProcessor(processor[0], outbox[0]);

        int[] restoreCount = {0};

        // do snapshot+restore before processing any item. This will test saveToSnapshot() in this edge case
        snapshotAndRestore(processor, outbox, actualOutputs, doSnapshots, doRestoreEvery, restoreCount);

        // call the process() method
        int ioPosition = 0;
        while (ioPosition < inputOutput.size() || !inbox.isEmpty()) {
            if (inbox.isEmpty()) {
                inboxOrdinal = inputOutput.get(ioPosition).ordinal();
                for (int added = 0;
                     ioPosition < inputOutput.size()
                             && added < testMode.inboxLimit()
                             && inputOutput.get(ioPosition).isInput()
                             && inboxOrdinal == inputOutput.get(ioPosition).ordinal
                             && (added == 0 || !(inputOutput.get(ioPosition).item instanceof Watermark));
                     added++
                ) {
                    ItemWithOrdinal objectWithOrdinal = inputOutput.get(ioPosition++);
                    inbox.queue().add(objectWithOrdinal.item);
                    inboxOrdinal = objectWithOrdinal.ordinal;
                }
                if (logInputOutput) {
                    System.out.println(LocalTime.now() + " Input-" + inboxOrdinal + ": " + inbox);
                }
            }
            int lastInboxSize = inbox.size();

            // add to accumulatedExpectedOutput
            while (ioPosition < inputOutput.size() && inputOutput.get(ioPosition).isOutput()) {
                accumulatedExpectedOutput.add(inputOutput.get(ioPosition));
                ioPosition++;
            }

            if (inbox.isEmpty()) {
                if (ioPosition < inputOutput.size()) {
                    throw new IllegalArgumentException("Invalid test case: there's expected output before first input -" +
                            " there's no processor call that could have produced that output, and we're not calling" +
                            " complete() yet");
                }
                break;
            }

            String methodName;
            methodName = processInbox(inbox, inboxOrdinal, isCooperative, processor);
            boolean madeProgress = inbox.size() < lastInboxSize ||
                (outbox[0].bucketCount() > 0 && !outbox[0].queue(0).isEmpty());
            assertTrue(methodName + "() call without progress", !assertProgress || madeProgress);
            idleCount = idle(idler, idleCount, madeProgress);
            if (outbox[0].bucketCount() > 0 && outbox[0].queue(0).size() == 1 && !inbox.isEmpty()) {
                // if the outbox is full, call the process() method again. Cooperative
                // processor must be able to cope with this situation and not try to put
                // more items to the outbox.
                outbox[0].reset();
                processInbox(inbox, inboxOrdinal, isCooperative, processor);
            }
            outbox[0].drainQueuesAndReset(actualOutputs, logInputOutput);
            if (inbox.isEmpty()) {
                if (outputMustOccurOnTime) {
                    // if there isn't more input to be processed, don't assert the output. The output after
                    // the last input item can be generated in `complete()`
                    if (ioPosition < inputOutput.size()) {
                        assertOutputFn.accept(testMode, actualOutputs);
                    }
                }
                snapshotAndRestore(processor, outbox, actualOutputs, doSnapshots, doRestoreEvery, restoreCount);
            }
        }

        if (logInputOutput && inputOutput.stream().anyMatch(ItemWithOrdinal::isInput)) {
            System.out.println(LocalTime.now() + " Input processed, calling complete()");
        }

        // call the complete() method
        if (callComplete) {
            long completeStart = System.nanoTime();
            long outputMatchedAt = Long.MAX_VALUE;
            boolean[] done = {false};
            do {
                doCall("complete", isCooperative, () -> done[0] = processor[0].complete());
                boolean madeProgress = done[0] ||
                    (outbox[0].bucketCount() > 0 && !outbox[0].queue(0).isEmpty());
                assertTrue("complete() call without progress", !assertProgress || madeProgress);
                outbox[0].drainQueuesAndReset(actualOutputs, logInputOutput);
                if (outbox[0].hasUnfinishedItem()) {
                    assertFalse("outbox has unfinished items, but complete() claims to be done", done[0]);
                    outbox[0].block();
                } else {
                    outbox[0].unblock();
                    snapshotAndRestore(processor, outbox, actualOutputs, madeProgress && doSnapshots && !done[0],
                            doRestoreEvery, restoreCount);
                }
                idleCount = idle(idler, idleCount, madeProgress);
                long now = System.nanoTime();
                if (runUntilOutputMatchesTimeoutMillis >= 0) {
                    try {
                        assertOutputFn.accept(testMode, actualOutputs);
                        outputMatchedAt = Math.min(outputMatchedAt, now);
                    } catch (AssertionError e) {
                        if (outputMatchedAt < Long.MAX_VALUE) {
                            throw new AssertionError("the output already matched, but doesn't match now", e);
                        }
                        // ignore the failure otherwise and continue calling complete()
                    }
                    long elapsedSinceStart = NANOSECONDS.toMillis(now - completeStart);
                    long elapsedSinceMatch = NANOSECONDS.toMillis(subtractClamped(now, outputMatchedAt));
                    if (elapsedSinceStart > runUntilOutputMatchesTimeoutMillis
                            || elapsedSinceMatch > runUntilOutputMatchesExtraTimeMillis) {
                        break;
                    }
                }
            } while (!done[0]);
            assertTrue("complete returned true in a run-until-output-matches mode",
                    !done[0] || runUntilOutputMatchesTimeoutMillis <= 0);
        }

        processor[0].close();

        assertOutputFn.accept(testMode, actualOutputs);
    }

    private void assertExpectedOutput(TestMode mode, List<List<?>> expected , List<List<Object>> actual) {
        for (int i = 0; i < expected.size(); i++) {
            List<?> expectedOutput = expected.get(i);
            List<?> actualOutput = actual.get(i);
            if (!outputChecker.test(expectedOutput, actualOutput)) {
                assertEquals("processor output in mode \"" + mode + "\" doesn't match",
                        "expected:\n" + listToString(expectedOutput),
                        "actual:\n" + listToString(actualOutput));
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
    private static List<ItemWithOrdinal> mixInputs(List<List<?>> inputs, int[] priorities) {
        SortedMap<Integer, List<Integer>> ordinalsByPriority = new TreeMap<>();
        for (int i = 0; i < priorities.length; i++) {
            ordinalsByPriority.computeIfAbsent(priorities[i], k -> new ArrayList<>())
                            .add(i);
        }

        List<ItemWithOrdinal> result = new ArrayList<>();
        for (List<Integer> ordinals : ordinalsByPriority.values()) {
            boolean allDone;
            int index = 0;
            do {
                allDone = true;
                for (Integer ordinal : ordinals) {
                    if (inputs.get(ordinal).size() > index) {
                        Object item = inputs.get(ordinal).get(index);
                        result.add(new ItemWithOrdinal(ordinal, item));
                        allDone = false;
                    }
                }
                index++;
            } while (!allDone);
        }

        return result;
    }

    private TestOutbox createOutbox() {
        return new TestOutbox(IntStream.generate(() -> 1).limit(outputOrdinalCount).toArray(), 1);
    }

    private String processInbox(TestInbox inbox, int inboxOrdinal, boolean isCooperative, Processor[] processor) {
        if (inbox.peek() instanceof Watermark) {
            Watermark wm = ((Watermark) inbox.peek());
            doCall("tryProcessWatermark", isCooperative, () -> {
                if (processor[0].tryProcessWatermark(wm)) {
                    inbox.remove();
                }
            });
            return "tryProcessWatermark";
        } else {
            doCall("process", isCooperative, () -> processor[0].process(inboxOrdinal, inbox));
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
        do {
            doCall("saveSnapshot", isCooperative, () -> done[0] = processor[0].saveToSnapshot());
            assertTrue("saveToSnapshot() call without progress",
                    !assertProgress || done[0] || !outbox[0].snapshotQueue().isEmpty()
                            || !outbox[0].queue(0).isEmpty());
            outbox[0].drainSnapshotQueueAndReset(snapshotInbox.queue(), false);
            outbox[0].drainQueuesAndReset(actualOutput, logInputOutput);
        } while (!done[0]);

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
            doCall("restoreSnapshot", isCooperative,
                    () -> processor[0].restoreFromSnapshot(snapshotInbox));
            assertTrue("restoreFromSnapshot() call without progress",
                    !assertProgress
                            || lastInboxSize > snapshotInbox.queue().size()
                            || !outbox[0].queue(0).isEmpty());
            outbox[0].drainQueuesAndReset(actualOutput, logInputOutput);
            lastInboxSize = snapshotInbox.queue().size();
        }
        do {
            doCall("finishSnapshotRestore", isCooperative,
                    () -> done[0] = processor[0].finishSnapshotRestore());
            assertTrue("finishSnapshotRestore() call without progress",
                    !assertProgress || done[0] || !outbox[0].queue(0).isEmpty());
            outbox[0].drainQueuesAndReset(actualOutput, logInputOutput);
        } while (!done[0]);
    }

    private void doCall(String methodName, boolean isCooperative, Runnable r) {
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
                System.out.printf("Warning: call to %s() took %.2fms, it should be <%dms normally%n",
                        methodName, toMillis(elapsed), COOPERATIVE_TIME_LIMIT_MS_WARN);
            }
        } else {
            if (elapsed > MILLISECONDS.toNanos(BLOCKING_TIME_LIMIT_MS_WARN)) {
                System.out.printf("Warning: call to %s() took %.2fms in non-cooperative processor. Is " +
                        "this expected?%n", methodName, toMillis(elapsed));
            }
        }
    }

    private void initProcessor(Processor processor, TestOutbox outbox) {
        SerializationService serializationService;
        if (hazelcastInstance != null && hazelcastInstance instanceof SerializationServiceSupport) {
            SerializationServiceSupport impl = (SerializationServiceSupport) hazelcastInstance;
            serializationService = impl.getSerializationService();
        } else {
            serializationService = new DefaultSerializationServiceBuilder()
                    .setManagedContext(e -> e)
                    .build();
        }

        TestProcessorContext context = new TestProcessorContext()
                .setLogger(getLogger(processor.getClass().getName()))
                .setManagedContext(serializationService.getManagedContext())
                .setLocalProcessorIndex(localProcessorIndex)
                .setGlobalProcessorIndex(globalProcessorIndex)
                .setLocalParallelism(localParallelism)
                .setTotalParallelism(totalParallelism);

        if (hazelcastInstance != null) {
            context.setHazelcastInstance(hazelcastInstance);
        }
        if (jobConfig != null) {
            context.setJobConfig(jobConfig);
        }
        if (processor instanceof SerializationServiceAware) {
            ((SerializationServiceAware) processor).setSerializationService(serializationService);
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

    private static ILogger getLogger(String name) {
        return LOGGING_SERVICE.getLogger(name);
    }

    /**
     * Converts a list to a string putting {@code toString()} of each element
     * on a separate line. It is useful to transform list inputs to {@code
     * assertEquals()}: the exception will show the entire collections instead
     * of just non-equal sizes or the first non-equal element.
     *
     * @param list Input list
     * @return Output string
     */
    private static String listToString(List<?> list) {
        return list.stream()
                   .map(obj -> {
                       if (obj instanceof Object[]) {
                           return Arrays.toString((Object[]) obj);
                       }
                       return String.valueOf(obj);
                   })
                   .collect(Collectors.joining("\n"));
    }

    public static final class ItemWithOrdinal {
        private final int ordinal;
        private final Object item;

        private ItemWithOrdinal(int ordinal, Object item) {
            this.ordinal = ordinal;
            this.item = item;
        }

        public boolean isInput() {
            return ordinal >= 0;
        }

        public boolean isOutput() {
            return ordinal < 0;
        }

        public int ordinal() {
            return isInput() ? ordinal : -ordinal - 1;
        }

        public Object item() {
            return item;
        }

        @Override
        public String toString() {
            return "ordinal=" + ordinal +
                    ", item=" + item;
        }
    }

    public static ItemWithOrdinal in(Object item) {
        return in(0, item);
    }

    public static ItemWithOrdinal in(int ordinal, Object item) {
        checkNotNegative(ordinal, "ordinal");
        return new ItemWithOrdinal(ordinal, item);
    }

    public static ItemWithOrdinal out(Object item) {
        return out(0, item);
    }

    public static ItemWithOrdinal out(int ordinal, Object item) {
        checkNotNegative(ordinal, "ordinal");
        return new ItemWithOrdinal(-ordinal - 1, item);
    }

    private List<List<?>> transformToListList(List<ItemWithOrdinal> items) {
        List<List<?>> res = new ArrayList<>();
        for (int i = 0; i < outputOrdinalCount; i++) {
            res.add(new ArrayList<>());
        }
        for (ItemWithOrdinal item : items) {
            assert item.isOutput();
            @SuppressWarnings("unchecked")
            List<Object> innerList = (List<Object>) res.get(item.ordinal());
            innerList.add(item.item);
        }
        return res;
    }

    /**
     * Describes the current test mode.
     */
    public static final class TestMode {

        private final boolean doSnapshots;
        private final int restoreInterval;
        private final int inboxLimit;

        /**
         * Construct a new instance.
         */
        private TestMode(boolean doSnapshots, int restoreInterval, int inboxLimit) {
            this.doSnapshots = doSnapshots;
            this.restoreInterval = restoreInterval;
            this.inboxLimit = inboxLimit;
        }

        /**
         * Are snapshots enabled.
         */
        public boolean isSnapshotsEnabled() {
            return doSnapshots;
        }

        /**
         * How often the snapshot is restored. 1 means restore every snapshot,
         * 2 every other snapshot.
         */
        public int snapshotRestoreInterval() {
            return restoreInterval;
        }

        /**
         * Size limit of the inbox.
         */
        public int inboxLimit() {
            return inboxLimit;
        }

        @Override
        public String toString() {
            String sInboxSize = inboxLimit == Integer.MAX_VALUE ? "unlimited" : String.valueOf(inboxLimit);
            if (!doSnapshots && restoreInterval == 0) {
                return "snapshots disabled, inboxLimit=" + sInboxSize;
            } else if (doSnapshots && restoreInterval == 1) {
                assert inboxLimit == 1;
                return "snapshots enabled, restoring every snapshot";
            } else if (doSnapshots && restoreInterval == 2) {
                assert inboxLimit == 1;
                return "snapshots enabled, restoring every other snapshot";
            } else if (doSnapshots && restoreInterval == Integer.MAX_VALUE) {
                return "snapshots enabled, never restoring them, inboxLimit=" + sInboxSize;
            } else {
                throw new IllegalArgumentException("Unknown mode, doSnapshots=" + doSnapshots + ", restoreInterval="
                    + restoreInterval + ", inboxLimit=" + inboxLimit);
            }
        }
    }
}
