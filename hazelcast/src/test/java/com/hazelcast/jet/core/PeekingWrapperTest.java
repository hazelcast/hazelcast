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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.PredicateEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.TestProcessors.ListSource;
import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.jet.impl.JetEvent;
import com.hazelcast.logging.ILogger;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import static com.hazelcast.jet.core.processor.DiagnosticProcessors.peekInputP;
import static com.hazelcast.jet.core.processor.DiagnosticProcessors.peekOutputP;
import static com.hazelcast.jet.core.processor.DiagnosticProcessors.peekSnapshotP;
import static com.hazelcast.jet.core.test.TestSupport.supplierFrom;
import static com.hazelcast.jet.impl.JetEvent.jetEvent;
import static com.hazelcast.jet.impl.util.Util.toLocalTime;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PeekingWrapperTest {

    private static final JetEvent<Integer> TEST_JET_EVENT = jetEvent(123, 2);

    @Parameter
    public String mode;

    private FunctionEx<Object, String> toStringFn;
    private PredicateEx<Object> shouldLogFn;

    private TestProcessorContext context;
    private ILogger logger;
    private Processor peekP;

    private FunctionEx<Entry<Integer, Integer>, String> snapshotToStringFn;
    private PredicateEx<Entry<Integer, Integer>> snapshotShouldLogFn;
    private String testJetEventString;

    @Parameters(name = "mode={0}")
    public static Collection<Object> parameters() {
        return asList("defaultFunctions", "customFunctions");
    }

    @Before
    public void before() {
        logger = mock(ILogger.class);
        context = new TestProcessorContext().setLogger(logger);
        if (mode.equals("customFunctions")) {
            toStringFn = (FunctionEx<Object, String>) o -> "a" + o;
            snapshotToStringFn = e -> toStringFn.apply(e.getKey()) + '=' + toStringFn.apply(e.getValue());
            shouldLogFn = (PredicateEx<Object>) o -> !(o instanceof Integer) || ((Integer) o) % 2 == 0;
            snapshotShouldLogFn = e -> shouldLogFn.test(e.getKey());
        } else {
            toStringFn = null;
            snapshotToStringFn = null;
            shouldLogFn = null;
            snapshotShouldLogFn = null;
        }
        testJetEventString = format(TEST_JET_EVENT)
                + " (eventTime=" + toLocalTime(TEST_JET_EVENT.timestamp()) + ")";
    }

    @Test
    public void when_peekInputWithPeekingProcessorSupplier() throws Exception {
        // Given
        SupplierEx<Processor> wrappedSupplier = procSupplier(TestPeekRemoveProcessor.class);
        peekP = (toStringFn == null
                ? peekInputP(wrappedSupplier)
                : peekInputP(toStringFn, shouldLogFn, wrappedSupplier)
        ).get();

        // When+Then
        assertPeekInput();
    }

    @Test
    public void when_peekInputWithPollingProcessorSupplier() throws Exception {
        // Given
        SupplierEx<Processor> passThroughPSupplier = procSupplier(TestPollProcessor.class);
        peekP = (toStringFn == null
                ? peekInputP(passThroughPSupplier)
                : peekInputP(toStringFn, shouldLogFn, passThroughPSupplier)
        ).get();

        // When+Then
        assertPeekInput();
    }

    @Test
    public void when_peekInputWithIteratingProcessorSupplier() throws Exception {
        // Given
        SupplierEx<Processor> passThroughPSupplier = procSupplier(TestIteratingProcessor.class);
        peekP = (toStringFn == null
                ? peekInputP(passThroughPSupplier)
                : peekInputP(toStringFn, shouldLogFn, passThroughPSupplier)
        ).get();

        // When+Then
        assertPeekInput();
    }

    @Test
    public void when_peekInput_processorSupplier() throws Exception {
        // Given
        ProcessorSupplier wrappedProcSupplier = ProcessorSupplier.of(procSupplier(TestPeekRemoveProcessor.class));
        ProcessorSupplier peekingProcSupplier = toStringFn == null
                ? peekInputP(wrappedProcSupplier)
                : peekInputP(toStringFn, shouldLogFn, wrappedProcSupplier);
        peekP = supplierFrom(peekingProcSupplier).get();

        // When+Then
        assertPeekInput();
    }

    @Test
    public void when_peekInput_metaSupplier() throws Exception {
        // Given
        ProcessorMetaSupplier wrappedMetaSupplier =
                ProcessorMetaSupplier.of(procSupplier(TestPeekRemoveProcessor.class));
        ProcessorMetaSupplier peekingMetaSupplier = toStringFn == null
                ? peekInputP(wrappedMetaSupplier)
                : peekInputP(toStringFn, shouldLogFn, wrappedMetaSupplier);
        peekP = supplierFrom(peekingMetaSupplier).get();

        // When+Then
        assertPeekInput();
    }

    private SupplierEx<Processor> peekOutputProcessorSupplier() {
        return () -> new ListSource(0, 1, new Watermark(2), TEST_JET_EVENT);
    }

    @Test
    public void when_peekOutput_supplier() throws Exception {
        // Given
        SupplierEx<Processor> passThroughPSupplier = peekOutputProcessorSupplier();
        peekP = (toStringFn == null
                ? peekOutputP(passThroughPSupplier)
                : peekOutputP(toStringFn, shouldLogFn, passThroughPSupplier)
        ).get();

        // When+Then
        assertPeekOutput();
    }

    @Test
    public void when_peekOutput_processorSupplier() throws Exception {
        // Given
        ProcessorSupplier wrappedProcSupplier = ProcessorSupplier.of(peekOutputProcessorSupplier());
        ProcessorSupplier peekingProcSupplier = toStringFn == null
                ? peekOutputP(wrappedProcSupplier)
                : peekOutputP(toStringFn, shouldLogFn, wrappedProcSupplier);
        peekP = peekingProcSupplier.get(1).iterator().next();

        // When+Then
        assertPeekOutput();
    }

    @Test
    public void when_peekOutput_metaSupplier() throws Exception {
        // Given
        ProcessorMetaSupplier sourceSupplier = ProcessorMetaSupplier.of(peekOutputProcessorSupplier());
        ProcessorMetaSupplier peekingMetaSupplier = toStringFn == null
                ? peekOutputP(sourceSupplier)
                : peekOutputP(toStringFn, shouldLogFn, sourceSupplier);
        peekP = supplierFrom(peekingMetaSupplier).get();

        // When+Then
        assertPeekOutput();
    }

    @Test
    public void when_peekSnapshot_supplier() throws Exception {
        // Given
        SupplierEx<Processor> wrappedSupplier = procSupplier(TestSourceProcessor.class);
        peekP = (toStringFn == null
                ? peekSnapshotP(wrappedSupplier)
                : peekSnapshotP(snapshotToStringFn, snapshotShouldLogFn, wrappedSupplier)
        ).get();

        // When+Then
        assertPeekSnapshot();
    }

    @Test
    public void when_peekSnapshot_procSupplier() throws Exception {
        // Given
        ProcessorSupplier wrappedProcSupplier = ProcessorSupplier.of(procSupplier(TestSourceProcessor.class));
        ProcessorSupplier peekingProcSupplier = toStringFn == null
                ? peekSnapshotP(wrappedProcSupplier)
                : peekSnapshotP(snapshotToStringFn, snapshotShouldLogFn, wrappedProcSupplier);
        peekP = supplierFrom(peekingProcSupplier).get();

        // When+Then
        assertPeekSnapshot();
    }

    @Test
    public void when_peekSnapshot_metaSupplier() throws Exception {
        // Given
        ProcessorMetaSupplier wrappedMetaSupplier = ProcessorMetaSupplier.of(procSupplier(TestSourceProcessor.class));
        ProcessorMetaSupplier peekingMetaSupplier = toStringFn == null
                ? peekSnapshotP(wrappedMetaSupplier)
                : peekSnapshotP(snapshotToStringFn, snapshotShouldLogFn, wrappedMetaSupplier);
        peekP = supplierFrom(peekingMetaSupplier).get();

        // When+Then
        assertPeekSnapshot();
    }

    private static SupplierEx<Processor> procSupplier(Class<? extends Processor> processor) {
        return () -> uncheckCall(processor::newInstance);
    }

    private void assertPeekInput() throws Exception {
        peekP.init(mock(Outbox.class), context);

        TestInbox inbox = new TestInbox();
        inbox.add(0);
        peekP.process(0, inbox);
        verify(logger).info("Input from ordinal 0: " + format(0));

        inbox.add(0);
        peekP.process(1, inbox);
        verify(logger).info("Input from ordinal 1: " + format(0));

        inbox.add(1);
        peekP.process(0, inbox);
        if (shouldLogFn == null) {
            verify(logger).info("Input from ordinal 0: " + format(1));
        } else {
            verifyZeroInteractions(logger);
        }

        Watermark wm = new Watermark(1);
        peekP.tryProcessWatermark(wm);
        verify(logger).info("Input: " + wm);

        inbox.add(TEST_JET_EVENT);
        peekP.process(0, inbox);
        verify(logger).info("Input from ordinal 0: " + testJetEventString);
    }

    private void assertPeekOutput() throws Exception {
        TestOutbox outbox = new TestOutbox(1, 1);
        peekP.init(outbox, context);

        Watermark wm = new Watermark(1);
        peekP.tryProcessWatermark(wm);
        verify(logger).info("Output to ordinal 0: " + wm);
        verify(logger).info("Output to ordinal 1: " + wm);
        outbox.queue(0).clear();
        outbox.queue(1).clear();
        outbox.reset();
        verifyZeroInteractions(logger);

        peekP.complete();
        verify(logger).info("Output to ordinal 0: " + format(0));
        verify(logger).info("Output to ordinal 1: " + format(0));

        outbox.queue(0).clear();
        outbox.queue(1).clear();
        outbox.reset();

        // only one queue has available space, call complete() again to emit another object
        peekP.complete();
        if (shouldLogFn == null) {
            verify(logger).info("Output to ordinal 1: " + format(1));
            verify(logger).info("Output to ordinal 0: " + format(1));
        }
        outbox.queue(0).clear();
        outbox.queue(1).clear();
        outbox.reset();
        verifyZeroInteractions(logger);

        peekP.complete();
        wm = new Watermark(2);
        verify(logger).info("Output to ordinal 0: " + wm);
        verify(logger).info("Output to ordinal 1: " + wm);
        outbox.queue(0).clear();
        outbox.queue(1).clear();
        outbox.reset();
        verifyZeroInteractions(logger);

        peekP.complete();
        verify(logger).info("Output to ordinal 0: " + testJetEventString);
        verify(logger).info("Output to ordinal 1: " + testJetEventString);
        verifyZeroInteractions(logger);
    }

    private void assertPeekSnapshot() throws Exception {
        TestOutbox outbox = new TestOutbox(new int[]{16}, 16);
        peekP.init(outbox, context);

        peekP.saveToSnapshot();
        verify(logger).info("Output to snapshot: " + formatEntry(0));

        // advance counter
        peekP.complete();
        if (shouldLogFn == null) {
            peekP.saveToSnapshot();
            verify(logger).info("Output to snapshot: " + formatEntry(1));
        }
        verifyZeroInteractions(logger);
    }

    private String format(Object s) {
        return toStringFn == null ? String.valueOf(s) : toStringFn.apply(s);
    }

    private String formatEntry(int s) {
        return toStringFn == null ? s + "=" + s : toStringFn.apply(s) + '=' + toStringFn.apply(s);
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
     * A processor that will process the inbox using inbox.peek() +
     * inbox.remove().
     */
    static class TestPeekRemoveProcessor extends TestProcessor {
        @Override
        public void process(int ordinal, @Nonnull Inbox inbox) {
            for (Object o; (o = inbox.peek()) != null; ) {
                assertEquals("second peek didn't return the same object", inbox.peek(), o);
                inbox.remove();
            }
            assertNull(inbox.peek());
            try {
                inbox.remove();
                fail("Remove didn't fail");
            } catch (NoSuchElementException expected) {
            }
        }

        @Override
        public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
            return true;
        }
    }

    /**
     * A processor that will process the inbox using inbox.poll().
     */
    static class TestPollProcessor extends TestProcessor {
        @Override
        public void process(int ordinal, @Nonnull Inbox inbox) {
            while (inbox.poll() != null) {
            }
        }

        @Override
        public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
            return true;
        }
    }

    /**
     * A processor that will process the inbox using inbox.iterator() +
     * inbox.clear().
     */
    static class TestIteratingProcessor extends TestProcessor {
        @Override
        public void process(int ordinal, @Nonnull Inbox inbox) {
            for (Object o : inbox) {
                assertNotNull("Inbox returned null object", o);
            }
            inbox.clear();
        }

        @Override
        public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
            return true;
        }
    }

    /**
     * A processor which emits a sequence of integers starting from 0.
     * On snapshot request, it saves the current counter.
     */
    static class TestSourceProcessor extends TestProcessor {
        private int counter;

        @Override
        public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
            return true;
        }

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
