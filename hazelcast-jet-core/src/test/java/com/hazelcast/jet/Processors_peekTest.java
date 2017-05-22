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
import com.hazelcast.jet.impl.execution.init.Contexts.ProcCtx;
import com.hazelcast.jet.impl.util.ArrayDequeInbox;
import com.hazelcast.jet.impl.util.ArrayDequeOutbox;
import com.hazelcast.jet.impl.util.ProgressTracker;
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

import static com.hazelcast.jet.Processors.peekInput;
import static com.hazelcast.jet.Processors.peekOutput;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;
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
    public DistributedFunction<Object, String> toStringF;

    @Parameter(1)
    public DistributedPredicate<Object> shouldLogF;

    @Parameter(2)
    public Class<Processor> processor;

    private ArrayDequeInbox inbox;
    private ArrayDequeOutbox outbox;
    private Processor.Context context;
    private ILogger logger;

    @Parameters(name = "toStringF={0}, shouldLogF={1}, processor={2}")
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
        inbox = new ArrayDequeInbox();
        outbox = new ArrayDequeOutbox(new int[]{128}, new ProgressTracker());
        logger = mock(ILogger.class);
        context = new ProcCtx(null, logger, null, 0);
    }

    private DistributedSupplier<Processor> procSupplier() {
        return () -> uncheckCall(() -> processor.newInstance());
    }

    @Test
    public void when_peekInput_SupplierProcessor() {
        // Given
        DistributedSupplier<Processor> passThroughPSupplier = procSupplier();
        Processor wrappedP = (toStringF == null
                ? peekInput(passThroughPSupplier)
                : peekInput(toStringF, shouldLogF, passThroughPSupplier)
        ).get();
        wrappedP.init(outbox, context);

        // When+Then
        assertLogged(wrappedP);
    }

    @Test
    public void when_peekInput_ProcessorSupplier() {
        // Given
        ProcessorSupplier passThroughPSupplier = ProcessorSupplier.of(procSupplier());
        Processor wrappedP = (toStringF == null
                ? peekInput(passThroughPSupplier)
                : peekInput(toStringF, shouldLogF, passThroughPSupplier)
        ).get(1).iterator().next();
        wrappedP.init(outbox, context);

        // When+Then
        assertLogged(wrappedP);
    }

    @Test
    public void when_peekInput_ProcessorMetaSupplier() {
        // Given
        ProcessorMetaSupplier passThroughPSupplier = ProcessorMetaSupplier.of(procSupplier());
        Address address = new Address();
        Processor wrappedP = (toStringF == null
                ? peekInput(passThroughPSupplier)
                : peekInput(toStringF, shouldLogF, passThroughPSupplier)
        ).get(Collections.singletonList(address)).apply(address).get(1).iterator().next();
        wrappedP.init(outbox, context);

        // When+Then
        assertLogged(wrappedP);
    }

    @Test
    public void when_peekOutput_SupplierProcessor() {
        // Given
        DistributedSupplier<Processor> passThroughPSupplier = procSupplier();
        Processor wrappedP = (toStringF == null
                ? peekInput(passThroughPSupplier)
                : peekOutput(toStringF, shouldLogF, passThroughPSupplier)
        ).get();

        wrappedP.init(outbox, context);

        // When+Then
        assertLogged(wrappedP);
    }

    @Test
    public void when_peekOutput_ProcessorSupplier() {
        // Given
        ProcessorSupplier passThroughPSupplier = ProcessorSupplier.of(procSupplier());
        Processor wrappedP = (toStringF == null
                ? peekInput(passThroughPSupplier)
                : peekOutput(toStringF, shouldLogF, passThroughPSupplier)
        ).get(1).iterator().next();
        wrappedP.init(outbox, context);

        // When+Then
        assertLogged(wrappedP);
    }

    @Test
    public void when_peekOutput_ProcessorMetaSupplier() {
        // Given
        ProcessorMetaSupplier passThroughPSupplier = ProcessorMetaSupplier.of(procSupplier());
        Address address = new Address();
        Processor wrappedP = (toStringF == null
                ? peekInput(passThroughPSupplier)
                : peekOutput(toStringF, shouldLogF, passThroughPSupplier)
        ).get(Collections.singletonList(address)).apply(address).get(1).iterator().next();
        wrappedP.init(outbox, context);

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
        if (shouldLogF == null) {
            verify(logger).info(format(2));
        }
        verifyZeroInteractions(logger);

        assertEquals(1, outbox.queueWithOrdinal(0).poll());
        assertEquals(2, outbox.queueWithOrdinal(0).poll());
        assertNull(outbox.queueWithOrdinal(0).poll());
    }

    private String format(int s) {
        return toStringF == null ? String.valueOf(s) : toStringF.apply(s);
    }

    abstract static class TestProcessor implements Processor {
        protected Outbox outbox;

        @Override
        public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
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
