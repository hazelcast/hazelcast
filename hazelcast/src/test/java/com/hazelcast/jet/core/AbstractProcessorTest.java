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

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor.FlatMapper;
import com.hazelcast.jet.core.TestProcessors.MockP;
import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.logging.ILogger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Queue;

import static java.util.stream.IntStream.range;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AbstractProcessorTest {

    private static final String MOCK_ITEM = "x";
    private static final int OUTBOX_BUCKET_COUNT = 4;
    private static final int ORDINAL_0 = 0;
    private static final int ORDINAL_1 = 1;
    private static final int ORDINAL_2 = 2;
    private static final int ORDINAL_3 = 3;
    private static final int ORDINAL_4 = 4;
    private static final int ORDINAL_5 = 5;
    private static final int[] ORDINALS_1_2 = {1, 2};
    private static final int[] ALL_ORDINALS = range(0, OUTBOX_BUCKET_COUNT).toArray();

    private RegisteringMethodCallsP p;
    private SpecializedByOrdinalP tryProcessP;

    private TestInbox inbox;
    private TestOutbox outbox;
    private NothingOverriddenP nothingOverriddenP;

    @Before
    public void before() throws Exception {
        inbox = new TestInbox();
        inbox.add(MOCK_ITEM);
        int[] capacities = new int[OUTBOX_BUCKET_COUNT];
        Arrays.fill(capacities, 1);
        outbox = new TestOutbox(capacities);
        final Processor.Context ctx = new TestProcessorContext();

        p = new RegisteringMethodCallsP();
        p.init(outbox, ctx);
        tryProcessP = new SpecializedByOrdinalP();
        tryProcessP.init(outbox, ctx);
        nothingOverriddenP = new NothingOverriddenP();
        nothingOverriddenP.init(outbox, ctx);
    }

    @Test
    public void when_init_then_customInitCalled() {
        assertTrue(tryProcessP.initCalled);
    }

    @Test
    public void when_init_then_loggerAvailable() {
        // When
        final ILogger logger = p.getLogger();

        // Then
        assertNotNull(logger);
    }

    @Test(expected = UnknownHostException.class)
    public void when_customInitThrows_then_initRethrows() throws Exception {
        new MockP().setInitError(new UnknownHostException())
                .init(mock(Outbox.class), new TestProcessorContext());
    }

    @Test
    public void when_processInbox0_then_tryProcess0Called() {
        // When
        tryProcessP.process(ORDINAL_0, inbox);

        // Then
        tryProcessP.validateReceptionOfItem(ORDINAL_0, MOCK_ITEM);
    }

    @Test
    public void when_processInbox1_then_tryProcess1Called() {
        // When
        tryProcessP.process(ORDINAL_1, inbox);

        // Then
        tryProcessP.validateReceptionOfItem(ORDINAL_1, MOCK_ITEM);
    }

    @Test
    public void when_processInbox2_then_tryProcess2Called() {
        // When
        tryProcessP.process(ORDINAL_2, inbox);

        // Then
        tryProcessP.validateReceptionOfItem(ORDINAL_2, MOCK_ITEM);
    }

    @Test
    public void when_processInbox3_then_tryProcess3Called() {
        // When
        tryProcessP.process(ORDINAL_3, inbox);

        // Then
        tryProcessP.validateReceptionOfItem(ORDINAL_3, MOCK_ITEM);
    }

    @Test
    public void when_processInbox4_then_tryProcess4Called() {
        // When
        tryProcessP.process(ORDINAL_4, inbox);

        // Then
        tryProcessP.validateReceptionOfItem(ORDINAL_4, MOCK_ITEM);
    }

    @Test
    public void when_processInbox5_then_tryProcessCalled() {
        // When
        tryProcessP.process(ORDINAL_5, inbox);

        // Then
        tryProcessP.validateReceptionOfItem(ORDINAL_5, MOCK_ITEM);
    }

    @Test(expected = UnknownHostException.class)
    public void when_processNThrows_then_processRethrows() throws Exception {
        // Given
        AbstractProcessor p = new AbstractProcessor() {
            @Override
            protected boolean tryProcess(int ordinal, @Nonnull Object item) throws UnknownHostException {
                throw new UnknownHostException();
            }
        };
        p.init(mock(Outbox.class), mock(Processor.Context.class));

        // When
        p.process(ORDINAL_0, inbox);

        // Then don't reach this line
    }

    @Test(expected = UnsupportedOperationException.class)
    public void when_tryProcess_then_unsupportedOperation() throws Exception {
        nothingOverriddenP.tryProcess(ORDINAL_0, MOCK_ITEM);
    }

    @Test
    public void when_tryProcess0_then_delegatesToTryProcess() throws Exception {
        // When
        boolean done = p.tryProcess0(MOCK_ITEM);

        // Then
        assertTrue(done);
        p.validateReceptionOfItem(ORDINAL_0, MOCK_ITEM);
    }

    @Test
    public void when_tryProcess1_then_delegatesToTryProcess() throws Exception {
        // When
        boolean done = p.tryProcess1(MOCK_ITEM);

        // Then
        assertTrue(done);
        p.validateReceptionOfItem(ORDINAL_1, MOCK_ITEM);
    }

    @Test
    public void when_tryProcess2_then_delegatesToTryProcess() throws Exception {
        // When
        boolean done = p.tryProcess2(MOCK_ITEM);

        // Then
        assertTrue(done);
        p.validateReceptionOfItem(ORDINAL_2, MOCK_ITEM);
    }

    @Test
    public void when_tryProcess3_then_delegatesToTryProcess() throws Exception {
        // When
        boolean done = p.tryProcess3(MOCK_ITEM);

        // Then
        assertTrue(done);
        p.validateReceptionOfItem(ORDINAL_3, MOCK_ITEM);
    }

    @Test
    public void when_tryProcess4_then_delegatesToTryProcess() throws Exception {
        // When
        boolean done = p.tryProcess4(MOCK_ITEM);

        // Then
        assertTrue(done);
        p.validateReceptionOfItem(ORDINAL_4, MOCK_ITEM);
    }

    @Test
    public void when_tryEmitToAll_then_emittedToAll() {
        // When
        boolean emitted = p.tryEmit(MOCK_ITEM);

        // Then
        assertTrue(emitted);
        validateReceptionAtOrdinals(MOCK_ITEM, ALL_ORDINALS);
    }

    @Test
    public void when_tryEmitTo1_then_emittedTo1() {
        // When
        boolean emitted = p.tryEmit(ORDINAL_1, MOCK_ITEM);

        // Then
        assertTrue(emitted);
        validateReceptionAtOrdinals(MOCK_ITEM, ORDINAL_1);
    }


    @Test
    public void when_tryEmitTo1And2_then_emittedTo1And2() {
        // When
        boolean emitted = p.tryEmit(ORDINALS_1_2, MOCK_ITEM);

        // Then
        assertTrue(emitted);
        validateReceptionAtOrdinals(MOCK_ITEM, ORDINALS_1_2);
    }

    @Test
    public void when_emitFromTraverserToAll_then_emittedToAll() {
        // Given
        Traverser<Object> trav = Traversers.singleton(MOCK_ITEM);

        // When
        boolean done = p.emitFromTraverser(trav);

        // Then
        assertTrue("done", done);
        validateReceptionAtOrdinals(MOCK_ITEM, ALL_ORDINALS);
    }

    @Test
    public void when_emitFromTraverserTo1_then_emittedTo1() {
        // Given
        Traverser<Object> trav = Traversers.traverseItems(MOCK_ITEM, MOCK_ITEM);

        boolean done;
        do {
            // When
            done = p.emitFromTraverser(ORDINAL_1, trav);
            // Then
            validateReceptionAtOrdinals(MOCK_ITEM, ORDINAL_1);
        } while (!done);
    }

    @Test
    public void when_emitFromTraverserTo1And2_then_emittedTo1And2() {
        // Given
        Traverser<Object> trav = Traversers.traverseItems(MOCK_ITEM, MOCK_ITEM);

        boolean done;
        do {
            // When
            done = p.emitFromTraverser(ORDINALS_1_2, trav);
            // Then
            validateReceptionAtOrdinals(MOCK_ITEM, ORDINALS_1_2);
        } while (!done);
    }

    @Test
    public void when_flatMapperTryProcessTo1And2_then_emittedTo1And2() {
        final Object item1 = 1;
        final Object item2 = 2;
        final int[] ordinals = {1, 2};
        final FlatMapper<String, Object> flatMapper = p.flatMapper(ordinals, x -> Traversers.traverseItems(item1, item2));

        // When
        boolean done = flatMapper.tryProcess(MOCK_ITEM);

        // Then
        assertFalse(done);
        validateReceptionAtOrdinals(item1, ordinals);

        // When
        done = flatMapper.tryProcess(MOCK_ITEM);

        // Then
        assertTrue(done);
        validateReceptionAtOrdinals(item2, ordinals);
    }

    @Test
    public void when_flatMapperTo1_then_emittedTo1() {
        // Given
        Object output = 42;
        FlatMapper<Object, Object> m = p.flatMapper(ORDINAL_1, x -> Traversers.singleton(output));

        // When
        boolean done = m.tryProcess(MOCK_ITEM);

        // Then
        assertTrue(done);
        validateReceptionAtOrdinals(output, ORDINAL_1);
    }

    @Test
    public void when_flatMapperToAll_then_emittedToAll() {
        // Given
        Object output = 42;
        FlatMapper<Object, Object> m = p.flatMapper(x -> Traversers.singleton(output));

        // When
        boolean done = m.tryProcess(MOCK_ITEM);

        // Then
        assertTrue(done);
        validateReceptionAtOrdinals(output, ALL_ORDINALS);
    }

    private void validateReceptionAtOrdinals(Object item, int... ordinals) {
        for (int i = 0; i < OUTBOX_BUCKET_COUNT; i++) {
            Queue<Object> q = outbox.queue(i);
            if (Util.arrayIndexOf(i, ordinals) >= 0) {
                assertEquals(item, q.poll());
            }
            assertNull(q.poll());
        }
        outbox.reset();
    }

    private static class RegisteringMethodCallsP extends AbstractProcessor {
        boolean initCalled;
        Object[] receivedByTryProcessN = new Object[6];

        @Override
        protected void init(@Nonnull Context context) {
            initCalled = true;
        }

        @Override
        protected boolean tryProcess(int ordinal, @Nonnull Object item) {
            receivedByTryProcessN[ordinal] = item;
            return true;
        }

        void validateReceptionOfItem(int ordinal, Object item) {
            for (int i = 0; i < receivedByTryProcessN.length; i++) {
                assertSame(i == ordinal ? item : null, receivedByTryProcessN[i]);
            }
        }
    }

    private static class SpecializedByOrdinalP extends RegisteringMethodCallsP {

        @Override
        protected boolean tryProcess(int ordinal, @Nonnull Object item) {
            assertEquals(5, ordinal);
            return super.tryProcess(ordinal, item);
        }

        @Override
        protected boolean tryProcess0(@Nonnull Object item) {
            receivedByTryProcessN[0] = item;
            return true;
        }

        @Override
        protected boolean tryProcess1(@Nonnull Object item) {
            receivedByTryProcessN[1] = item;
            return true;
        }

        @Override
        protected boolean tryProcess2(@Nonnull Object item) {
            receivedByTryProcessN[2] = item;
            return true;
        }

        @Override
        protected boolean tryProcess3(@Nonnull Object item) {
            receivedByTryProcessN[3] = item;
            return true;
        }

        @Override
        protected boolean tryProcess4(@Nonnull Object item) {
            receivedByTryProcessN[4] = item;
            return true;
        }
    }

    private static class NothingOverriddenP extends AbstractProcessor {
    }
}
