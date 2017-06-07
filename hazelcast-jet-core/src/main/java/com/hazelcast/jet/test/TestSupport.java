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

package com.hazelcast.jet.test;

import com.hazelcast.config.NetworkConfig;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorMetaSupplier;
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.nio.Address;

import javax.annotation.Nonnull;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import static com.hazelcast.jet.test.JetAssert.assertEquals;
import static com.hazelcast.jet.test.JetAssert.assertTrue;
import static java.util.Collections.singletonList;

/**
 * Utilities to write unit tests.
 */
public final class TestSupport {

    private static final Address LOCAL_ADDRESS;

    static {
        try {
            LOCAL_ADDRESS = new Address("localhost", NetworkConfig.DEFAULT_PORT);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    private TestSupport() {
    }

    /**
     * Convenience for {@link #testProcessor(Processor, List, List, boolean)}
     * with progress assertion enabled.
     */
    public static <T, U> void testProcessor(@Nonnull DistributedSupplier<Processor> supplier,
                                            @Nonnull List<T> input, @Nonnull List<U> expectedOutput) {
        testProcessor(processorFrom(supplier), input, expectedOutput, true);
    }

    /**
     * Convenience for {@link #testProcessor(Processor, List, List, boolean)}
     * with progress assertion enabled.
     */
    public static <T, U> void testProcessor(@Nonnull ProcessorSupplier supplier,
                                            @Nonnull List<T> input, @Nonnull List<U> expectedOutput) {
        testProcessor(processorFrom(supplier), input, expectedOutput, true);
    }

    /**
     * Convenience for {@link #testProcessor(Processor, List, List, boolean)}
     * with progress assertion enabled.
     */
    public static <T, U> void testProcessor(@Nonnull ProcessorMetaSupplier supplier,
                                            @Nonnull List<T> input, @Nonnull List<U> expectedOutput) {
        testProcessor(processorFrom(supplier), input, expectedOutput, true);
    }

    /**
     * Convenience for {@link #testProcessor(Processor, List, List, boolean)}
     * with progress assertion enabled.
     */
    public static <T, U> void testProcessor(@Nonnull Processor processor, @Nonnull List<T> input,
                                            @Nonnull List<U> expectedOutput) {
        testProcessor(processor, input, expectedOutput, true);
    }

    /**
     * Convenience for {@link #testProcessor(Processor, List, List, boolean)}
     * extracting the processor instance from the {@code supplier}.
     */
    public static <T, U> void testProcessor(
            @Nonnull DistributedSupplier<Processor> supplier,
            @Nonnull List<T> input,
            @Nonnull List<U> expectedOutput,
            boolean assertProgress
    ) {
        testProcessor(processorFrom(supplier), input, expectedOutput, assertProgress);
    }

    /**
     * Convenience for {@link #testProcessor(Processor, List, List, boolean)}
     * extracting the processor instance from the {@code supplier}.
     */
    public static <T, U> void testProcessor(
            @Nonnull ProcessorSupplier supplier,
            @Nonnull List<T> input,
            @Nonnull List<U> expectedOutput,
            boolean assertProgress
    ) {
        testProcessor(processorFrom(supplier), input, expectedOutput, assertProgress);
    }

    /**
     * Convenience for {@link #testProcessor(Processor, List, List, boolean)}
     * extracting the processor instance from the {@code supplier}.
     */
    public static <T, U> void testProcessor(
            @Nonnull ProcessorMetaSupplier supplier,
            @Nonnull List<T> input,
            @Nonnull List<U> expectedOutput,
            boolean assertProgress
    ) {
        testProcessor(processorFrom(supplier), input, expectedOutput, assertProgress);
    }

    /**
     * A utility to test processors. It will initialize the processor instance,
     * pass input items to it and assert the outbox contents.
     * <p>
     * This method does the following:<ul>
     *     <li>initializes the processor by calling {@link Processor#init(
     *     com.hazelcast.jet.Outbox, com.hazelcast.jet.Processor.Context)}
     *
     *     <li>calls {@link Processor#process(int, com.hazelcast.jet.Inbox)
     *     Processor.process(0, inbox)}, the inbox contains all items from
     *     {@code input} parameter
     *
     *     <li>asserts the progress of the {@code process()} call: that
     *     something was taken from the inbox or put to the outbox
     *
     *     <li>calls {@link Processor#complete()} until it returns {@code true}
     *
     *     <li>asserts the progress of the {@code complete()} call if it
     *     returned {@code false}: something must have been put to the outbox.
     * </ul>
     * Note that this method never calls {@link Processor#tryProcess()}.
     * <p>
     * For cooperative processors a 1-capacity outbox will be provided, which
     * will additionally be full in every other call to {@code process()}. This
     * will test the edge case: the {@code process()} method is called even
     * when the outbox is full to give the processor a chance to process inbox.
     * <p>
     * This class does not cover these cases:<ul>
     *     <li>Testing of processors which distinguish input or output edges
     *     by ordinal
     *     <li>Checking that the state of a stateful processor is empty at the
     *     end (you can do that yourself afterwards).
     * </ul>
     *
     * Example usage. This will test one of the jet-provided processors:
     * <pre>{@code
     * TestSupport.testProcessor(
     *         Processors.map((String s) -> s.toUpperCase()),
     *         asList("foo", "bar"),
     *         asList("FOO", "BAR")
     * );
     * }</pre>
     *
     * @param processor a processor instance to test
     * @param input input to pass
     * @param expectedOutput expected output
     * @param assertProgress if false, progress will not be asserted after
     *                       {@code process()} and {@code complete()} calls
     * @param <T> input items type
     * @param <U> output items type
     *
     * @see #testProcessor(Processor, List, List, boolean)
     */
    public static <T, U> void testProcessor(@Nonnull Processor processor, @Nonnull List<T> input,
                                            @Nonnull List<U> expectedOutput, boolean assertProgress) {
        TestInbox inbox = new TestInbox();
        inbox.addAll(input);

        // we'll use 1-capacity outbox to test cooperative emission, if the processor is cooperative
        int outboxCapacity = processor.isCooperative() ? 1 : Integer.MAX_VALUE;
        TestOutbox outbox = new TestOutbox(outboxCapacity);
        Queue<Object> bucket = outbox.queueWithOrdinal(0);
        List<Object> actualOutput = new ArrayList<>();

        // create instance of your processor and call the init() method
        processor.init(outbox, new TestProcessorContext());

        // call the process() method
        int lastInboxSize = inbox.size();
        int lastOutboxSize = bucket.size();
        while (!inbox.isEmpty()) {
            processor.process(0, inbox);
            if (processor.isCooperative() && bucket.size() == 1) {
                // if the outbox is full, call the process() method again. Cooperative
                // processor must be able to cope with this situation and not try to put
                // more items to the outbox.
                processor.process(0, inbox);
            }
            drainOutbox(bucket, actualOutput);
            if (assertProgress) {
                assertTrue("process() call without progress",
                        lastInboxSize > inbox.size() || lastOutboxSize < actualOutput.size());
            }
            lastInboxSize = inbox.size();
            lastOutboxSize = actualOutput.size();
        }

        // call the complete() method
        boolean done;
        do {
            done = processor.complete();
            drainOutbox(bucket, actualOutput);
            if (assertProgress) {
                assertTrue("complete() call without progress", done || lastOutboxSize < actualOutput.size());
            }
            lastOutboxSize = actualOutput.size();
        } while (!done);

        // assert the outbox
        assertEquals("processor output doesn't match", expectedOutput, actualOutput);
    }

    /**
     * Move all items from the outbox to the {@code outputList}.
     * @param outboxBucket the queue from Outbox to drain
     * @param outputList target list
     */
    public static void drainOutbox(Queue<Object> outboxBucket, List<Object> outputList) {
        for (Object o; (o = outboxBucket.poll()) != null; ) {
            outputList.add(o);
        }
    }

    /**
     * Gets single processor instance from processor supplier.
     */
    public static Processor processorFrom(DistributedSupplier<Processor> supplier) {
        return supplier.get();
    }

    /**
     * Gets single processor instance from processor supplier.
     */
    public static Processor processorFrom(ProcessorSupplier supplier) {
        supplier.init(new TestProcessorSupplierContext());
        return supplier.get(1).iterator().next();
    }

    /**
     * Gets single processor instance from meta processor supplier.
     */
    public static Processor processorFrom(ProcessorMetaSupplier supplier) {
        supplier.init(new TestProcessorMetaSupplierContext());
        return processorFrom(supplier.get(singletonList(LOCAL_ADDRESS)).apply(LOCAL_ADDRESS));
    }
}
