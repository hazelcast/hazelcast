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

import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import static com.hazelcast.jet.core.processor.DiagnosticProcessors.peekInput;
import static com.hazelcast.jet.core.processor.DiagnosticProcessors.peekOutput;
import static com.hazelcast.jet.core.processor.DiagnosticProcessors.peekSnapshot;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

@RunWith(Parameterized.class)
@Category({QuickTest.class})
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
public class Processors_peekTest {

    @Parameter
    public DistributedFunction<Object, String> toStringFn;

    @Parameter(1)
    public DistributedPredicate<Object> shouldLogFn;

    private TestProcessorContext context;
    private ILogger logger;
    private Processor wrappedP;

    private final DistributedFunction<Entry<Integer, Integer>, String> ssToStringFn = e ->
            toStringFn.apply(e.getKey()) + "=" + toStringFn.apply(e.getValue());
    private final DistributedPredicate<Entry<Integer, Integer>> ssShouldLogFn = e ->
            shouldLogFn.test(e.getKey());

    @Parameters(name = "toStringFn={0}, shouldLogFn={1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
                new Object[]{null, null},
                new Object[]{
                        (DistributedFunction<Integer, String>) o -> "a" + o,
                        (DistributedPredicate<Integer>) (Integer o) -> o % 2 == 0,
                }
        );
    }

    @Before
    public void before() {
        logger = mock(ILogger.class);
        context = new TestProcessorContext().setLogger(logger);
    }

    private DistributedSupplier<Processor> procSupplier(Class<? extends Processor> processor) {
        return () -> uncheckCall(() -> processor.newInstance());
    }

    @Test
    public void when_peekInputWithPeekingProcessor_SupplierProcessor() {
        // Given
        DistributedSupplier<Processor> passThroughPSupplier = procSupplier(TestPeekRemoveProcessor.class);
        wrappedP = (toStringFn == null
                ? peekInput(passThroughPSupplier)
                : peekInput(toStringFn, shouldLogFn, passThroughPSupplier)
        ).get();

        // When+Then
        assertPeekInput();
    }

    @Test
    public void when_peekInputWithPollingProcessor_SupplierProcessor() {
        // Given
        DistributedSupplier<Processor> passThroughPSupplier = procSupplier(TestPollProcessor.class);
        wrappedP = (toStringFn == null
                ? peekInput(passThroughPSupplier)
                : peekInput(toStringFn, shouldLogFn, passThroughPSupplier)
        ).get();

        // When+Then
        assertPeekInput();
    }

    @Test
    public void when_peekInput_ProcessorSupplier() {
        // Given
        ProcessorSupplier passThroughPSupplier = ProcessorSupplier.of(procSupplier(TestPeekRemoveProcessor.class));
        wrappedP = (toStringFn == null
                ? peekInput(passThroughPSupplier)
                : peekInput(toStringFn, shouldLogFn, passThroughPSupplier)
        ).get(1).iterator().next();

        // When+Then
        assertPeekInput();
    }

    @Test
    public void when_peekInput_ProcessorMetaSupplier() {
        // Given
        ProcessorMetaSupplier passThroughPSupplier = ProcessorMetaSupplier.of(procSupplier(TestPeekRemoveProcessor.class));
        Address address = new Address();
        wrappedP = (toStringFn == null
                ? peekInput(passThroughPSupplier)
                : peekInput(toStringFn, shouldLogFn, passThroughPSupplier)
        ).get(Collections.singletonList(address)).apply(address).get(1).iterator().next();

        // When+Then
        assertPeekInput();
    }

    @Test
    public void when_peekOutput_SupplierProcessor() {
        // Given
        DistributedSupplier<Processor> passThroughPSupplier = procSupplier(TestSourceProcessor.class);
        wrappedP = (toStringFn == null
                ? peekOutput(passThroughPSupplier)
                : peekOutput(toStringFn, shouldLogFn, passThroughPSupplier)
        ).get();

        // When+Then
        assertPeekOutput();
    }

    @Test
    public void when_peekOutput_ProcessorSupplier() {
        // Given
        ProcessorSupplier passThroughPSupplier = ProcessorSupplier.of(procSupplier(TestSourceProcessor.class));
        wrappedP = (toStringFn == null
                ? peekOutput(passThroughPSupplier)
                : peekOutput(toStringFn, shouldLogFn, passThroughPSupplier)
        ).get(1).iterator().next();

        // When+Then
        assertPeekOutput();
    }

    @Test
    public void when_peekOutput_ProcessorMetaSupplier() {
        // Given
        ProcessorMetaSupplier passThroughPSupplier = ProcessorMetaSupplier.of(procSupplier(TestSourceProcessor.class));
        Address address = new Address();
        wrappedP = (toStringFn == null
                ? peekOutput(passThroughPSupplier)
                : peekOutput(toStringFn, shouldLogFn, passThroughPSupplier)
        ).get(Collections.singletonList(address)).apply(address).get(1).iterator().next();

        // When+Then
        assertPeekOutput();
    }

    @Test
    public void when_peekSnapshot_SupplierProcessor() {
        // Given
        DistributedSupplier<Processor> passThroughPSupplier = procSupplier(TestSourceProcessor.class);
        wrappedP = (toStringFn == null
                ? peekSnapshot(passThroughPSupplier)
                : peekSnapshot(ssToStringFn, ssShouldLogFn, passThroughPSupplier)
        ).get();

        // When+Then
        assertPeekSnapshot();
    }

    @Test
    public void when_peekSnapshot_ProcessorSupplier() {
        // Given
        ProcessorSupplier passThroughPSupplier = ProcessorSupplier.of(procSupplier(TestSourceProcessor.class));
        wrappedP = (toStringFn == null
                ? peekSnapshot(passThroughPSupplier)
                : peekSnapshot(ssToStringFn, ssShouldLogFn, passThroughPSupplier)
        ).get(1).iterator().next();

        // When+Then
        assertPeekSnapshot();
    }

    @Test
    public void when_peekSnapshot_ProcessorMetaSupplier() {
        // Given
        ProcessorMetaSupplier passThroughPSupplier = ProcessorMetaSupplier.of(procSupplier(TestSourceProcessor.class));
        Address address = new Address();
        wrappedP = (toStringFn == null
                ? peekSnapshot(passThroughPSupplier)
                : peekSnapshot(ssToStringFn, ssShouldLogFn, passThroughPSupplier)
        ).get(Collections.singletonList(address)).apply(address).get(1).iterator().next();

        // When+Then
        assertPeekSnapshot();
    }

    private void assertPeekInput() {
        wrappedP.init(mock(Outbox.class), context);

        TestInbox inbox = new TestInbox();
        inbox.add(0);
        wrappedP.process(0, inbox);
        verify(logger).info("Input from 0: " + format(0));

        inbox.add(0);
        wrappedP.process(1, inbox);
        verify(logger).info("Input from 1: " + format(0));

        inbox.add(1);
        wrappedP.process(0, inbox);
        if (shouldLogFn == null) {
            verify(logger).info("Input from 0: " + format(1));
        }
        verifyZeroInteractions(logger);
    }

    private void assertPeekOutput() {
        TestOutbox outbox = new TestOutbox(1, 1);
        wrappedP.init(outbox, context);

        wrappedP.complete();
        verify(logger).info("Output to 0: " + format(0));
        verify(logger).info("Output to 1: " + format(0));

        outbox.queueWithOrdinal(1).clear();

        if (shouldLogFn == null) {
            // only one queue has available space
            wrappedP.complete();
            verify(logger).info("Output to 1: " + format(1));

            outbox.queueWithOrdinal(0).clear();
            wrappedP.complete();
            verify(logger).info("Output to 0: " + format(1));
        }
        verifyZeroInteractions(logger);
    }

    private void assertPeekSnapshot() {
        TestOutbox outbox = new TestOutbox(new int[]{16}, 16);
        wrappedP.init(outbox, context);

        wrappedP.saveToSnapshot();
        verify(logger).info("Output to snapshot: " + formatEntry(0));

        // advance counter
        wrappedP.complete();
        if (shouldLogFn == null) {
            wrappedP.saveToSnapshot();
            verify(logger).info("Output to snapshot: " + formatEntry(1));
        }
        verifyZeroInteractions(logger);
    }

    private String format(int s) {
        return toStringFn == null ? String.valueOf(s) : toStringFn.apply(s);
    }

    private String formatEntry(int s) {
        return toStringFn == null ? s + "=" + s : toStringFn.apply(s) + "=" + toStringFn.apply(s);
    }

    abstract static class TestProcessor implements Processor {
        Outbox outbox;

        @Override
        public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
            this.outbox = outbox;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName();
        }
    }

    /**
     * A processor that will pass through inbox to outbox using inbox.peek() + inbox.remove()
     */
    static class TestPeekRemoveProcessor extends TestProcessor {

        @Override
        public void process(int ordinal, @Nonnull Inbox inbox) {
            for (Object o; (o = inbox.peek()) != null; ) {
                assertNotNull("Inbox returned null object", o);
                assertEquals("second peek didn't return the same object", inbox.peek(), o);
                assertEquals("remove didn't return the same object", inbox.remove(), o);
            }
            assertNull(inbox.peek());
            try {
                inbox.remove();
                fail("Remove didn't fail");
            } catch (NoSuchElementException expected) {
            }
        }
    }

    /**
     * A processor that will pass through inbox to outbox using inbox.poll()
     */
    static class TestPollProcessor extends TestProcessor {
        @Override
        public void process(int ordinal, @Nonnull Inbox inbox) {
            for (Object o; (o = inbox.poll()) != null; ) {
                assertNotNull("Inbox returned null object", o);
            }

            assertNull(inbox.poll());
        }
    }

    /**
     * A processor which emits a sequence of integers starting from 0.
     * On snapshot request, it saves the current counter.
     */
    static class TestSourceProcessor extends TestProcessor {
        private int counter;

        @Override
        public boolean complete() {
            if (outbox.offer(counter)) {
                counter++;
            }
            return false;
        }

        @Override
        public boolean saveToSnapshot() {
            return outbox.offerToSnapshot(counter, counter);
        }
    }
}
