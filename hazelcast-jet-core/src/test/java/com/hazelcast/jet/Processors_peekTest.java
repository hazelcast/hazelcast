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

import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.test.TestInbox;
import com.hazelcast.jet.test.TestOutbox;
import com.hazelcast.jet.test.TestProcessorContext;
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
import java.util.NoSuchElementException;

import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.jet.processor.DiagnosticProcessors.peekInput;
import static com.hazelcast.jet.processor.DiagnosticProcessors.peekOutput;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
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

    @Parameter(2)
    public Class<Processor> processor;

    private TestInbox inbox;
    private TestOutbox outbox;
    private TestProcessorContext context;
    private ILogger logger;

    @Parameters(name = "toStringFn={0}, shouldLogFn={1}, processor={2}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
                new Object[]{null, null, TestPeekRemoveProcessor.class},
                new Object[]{null, null, TestPollProcessor.class},
                new Object[]{
                        (DistributedFunction<Object, String>) o -> "a" + o,
                        (DistributedPredicate<Object>) o -> (int) o <= 1,
                        TestPeekRemoveProcessor.class
                }
        );
    }

    @Before
    public void before() {
        inbox = new TestInbox();
        outbox = new TestOutbox(128);
        logger = mock(ILogger.class);
        context = new TestProcessorContext().setLogger(logger);
    }

    private DistributedSupplier<Processor> procSupplier() {
        return () -> uncheckCall(() -> processor.newInstance());
    }

    @Test
    public void when_peekInput_SupplierProcessor() {
        // Given
        DistributedSupplier<Processor> passThroughPSupplier = procSupplier();
        Processor wrappedP = (toStringFn == null
                ? peekInput(passThroughPSupplier)
                : peekInput(toStringFn, shouldLogFn, passThroughPSupplier)
        ).get();
        wrappedP.init(outbox, outbox, context);

        // When+Then
        assertLogged(wrappedP);
    }

    @Test
    public void when_peekInput_ProcessorSupplier() {
        // Given
        ProcessorSupplier passThroughPSupplier = ProcessorSupplier.of(procSupplier());
        Processor wrappedP = (toStringFn == null
                ? peekInput(passThroughPSupplier)
                : peekInput(toStringFn, shouldLogFn, passThroughPSupplier)
        ).get(1).iterator().next();
        wrappedP.init(outbox, outbox, context);

        // When+Then
        assertLogged(wrappedP);
    }

    @Test
    public void when_peekInput_ProcessorMetaSupplier() {
        // Given
        ProcessorMetaSupplier passThroughPSupplier = ProcessorMetaSupplier.of(procSupplier());
        Address address = new Address();
        Processor wrappedP = (toStringFn == null
                ? peekInput(passThroughPSupplier)
                : peekInput(toStringFn, shouldLogFn, passThroughPSupplier)
        ).get(Collections.singletonList(address)).apply(address).get(1).iterator().next();
        wrappedP.init(outbox, outbox, context);

        // When+Then
        assertLogged(wrappedP);
    }

    @Test
    public void when_peekOutput_SupplierProcessor() {
        // Given
        DistributedSupplier<Processor> passThroughPSupplier = procSupplier();
        Processor wrappedP = (toStringFn == null
                ? peekOutput(passThroughPSupplier)
                : peekOutput(toStringFn, shouldLogFn, passThroughPSupplier)
        ).get();

        wrappedP.init(outbox, outbox, context);

        // When+Then
        assertLogged(wrappedP);
    }

    @Test
    public void when_peekOutput_ProcessorSupplier() {
        // Given
        ProcessorSupplier passThroughPSupplier = ProcessorSupplier.of(procSupplier());
        Processor wrappedP = (toStringFn == null
                ? peekOutput(passThroughPSupplier)
                : peekOutput(toStringFn, shouldLogFn, passThroughPSupplier)
        ).get(1).iterator().next();
        wrappedP.init(outbox, outbox, context);

        // When+Then
        assertLogged(wrappedP);
    }

    @Test
    public void when_peekOutput_ProcessorMetaSupplier() {
        // Given
        ProcessorMetaSupplier passThroughPSupplier = ProcessorMetaSupplier.of(procSupplier());
        Address address = new Address();
        Processor wrappedP = (toStringFn == null
                ? peekOutput(passThroughPSupplier)
                : peekOutput(toStringFn, shouldLogFn, passThroughPSupplier)
        ).get(Collections.singletonList(address)).apply(address).get(1).iterator().next();
        wrappedP.init(outbox, outbox, context);

        // When+Then
        assertLogged(wrappedP);
    }

    private void assertLogged(Processor wrappedP) {
        // When
        inbox.add(1);
        inbox.add(2);
        wrappedP.process(0, inbox);

        // Then
        verify(logger).info(format(1));
        if (shouldLogFn == null) {
            verify(logger).info(format(2));
        }
        verifyZeroInteractions(logger);

        assertEquals(1, outbox.queueWithOrdinal(0).poll());
        assertEquals(2, outbox.queueWithOrdinal(0).poll());
        assertNull(outbox.queueWithOrdinal(0).poll());
    }

    private String format(int s) {
        return toStringFn == null ? String.valueOf(s) : toStringFn.apply(s);
    }

    abstract static class TestProcessor implements Processor {
        protected Outbox outbox;

        @Override
        public void init(@Nonnull Outbox outbox, @Nonnull SnapshotOutbox snapshotOutbox, @Nonnull Context context) {
            this.outbox = outbox;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName();
        }
    }

    /** A processor that will pass through inbox to outbox using inbox.peek() + inbox.remove() */
    static class TestPeekRemoveProcessor extends TestProcessor {

        @Override
        public void process(int ordinal, @Nonnull Inbox inbox) {
            for (Object o; (o = inbox.peek()) != null; ) {
                assertEquals("second peek didn't return the same object", inbox.peek(), o);
                assertTrue(outbox.offer(o));
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

    /** A processor that will pass through inbox to outbox using inbox.poll() */
    static class TestPollProcessor extends TestProcessor {
        @Override
        public void process(int ordinal, @Nonnull Inbox inbox) {
            for (Object o; (o = inbox.poll()) != null; ) {
                assertTrue(outbox.offer(o));
            }

            assertNull(inbox.poll());
        }
    }
}
