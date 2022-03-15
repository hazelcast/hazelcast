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
import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.TestProcessors.MockP;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import static com.hazelcast.jet.core.processor.Processors.noopP;
import static com.hazelcast.jet.core.test.TestSupport.SAME_ITEMS_ANY_ORDER;
import static com.hazelcast.jet.core.test.TestSupport.verifyProcessor;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TestSupportTest {

    @Test
    public void test_compareAsSet() {
        assertTrue(SAME_ITEMS_ANY_ORDER.test(asList("a", "b", "a"), asList("a", "a", "b")));
        assertFalse(SAME_ITEMS_ANY_ORDER.test(asList("a", "b", "a"), asList("a", "b", "b")));
    }

    @Test
    public void when_processorSupplierTested_then_completeCalled() {
        boolean[] completeCalled = {false};

        ProcessorSupplier supplier = new ProcessorSupplier() {
            @Nonnull
            @Override
            public Collection<? extends Processor> get(int count) {
                assertEquals(1, count);
                return singletonList(noopP().get());
            }

            @Override
            public void close(Throwable error) {
                completeCalled[0] = true;
            }
        };

        TestSupport
                .verifyProcessor(supplier)
                .expectOutput(emptyList());

        assertTrue("PS.complete not called", completeCalled[0]);

        // test once more with PMS
        completeCalled[0] = false;
        TestSupport
                .verifyProcessor(ProcessorMetaSupplier.of(supplier))
                .expectOutput(emptyList());

        assertTrue("PS.complete not called when using PMS", completeCalled[0]);
    }

    @Test
    public void test_processorMetaSupplierHasJetInstance() {
        HazelcastInstance hazelcastInstance = mockHazelcastInstance();
        boolean[] called = {false};

        verifyProcessor(
                new ProcessorMetaSupplier() {
                    @Override
                    public void init(@Nonnull Context context) {
                        assertSame(context.hazelcastInstance(), hazelcastInstance);
                        called[0] = true;
                    }

                    @Nonnull
                    @Override
                    public Function<? super Address, ? extends ProcessorSupplier> get(@Nonnull List<Address> addresses) {
                        return a -> ProcessorSupplier.of(MockP::new);
                    }
                })
                .hazelcastInstance(hazelcastInstance)
                .expectOutput(emptyList());

        assertTrue(called[0]);
    }

    @Test
    public void test_processorSupplierHasJetInstance() {
        HazelcastInstance hazelcastInstance = mockHazelcastInstance();

        boolean[] called = {false};

        verifyProcessor(
                new ProcessorSupplier() {
                    @Override
                    public void init(@Nonnull Context context) {
                        assertSame(context.hazelcastInstance(), hazelcastInstance);
                        called[0] = true;
                    }

                    @Nonnull
                    @Override
                    public Collection<? extends Processor> get(int count) {
                        assertEquals(1, count);
                        return singletonList(new MockP());
                    }
                })
                .hazelcastInstance(hazelcastInstance)
                .expectOutput(emptyList());

        assertTrue(called[0]);
    }

    private HazelcastInstance mockHazelcastInstance() {
        HazelcastInstance hzInstance = mock(HazelcastInstance.class);
        Cluster cluster = mock(Cluster.class);
        Member localMember = mock(Member.class);

        when(hzInstance.getCluster()).thenReturn(cluster);
        when(cluster.getLocalMember()).thenReturn(localMember);
        return hzInstance;
    }
}
