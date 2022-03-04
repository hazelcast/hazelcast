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

import com.hazelcast.cluster.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.function.Function;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class VertexTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private Vertex v;

    @Test
    public void when_constructed_then_hasDefaultParallelism() {
        // When
        v = new Vertex("v", NoopP::new);

        // Then
        assertEquals(-1, v.getLocalParallelism());
    }

    @Test
    public void when_constructWithName_then_hasThatName() {
        // When
        v = new Vertex("v", NoopP::new);

        // Then
        assertEquals("v", v.getName());
    }

    @Test
    public void when_constructWithSimpleSupplier_then_suppliesCorrectProcessor() throws Exception {
        // When
        v = new Vertex("v", NoopP::new);

        // Then
        validateProcessor();
    }

    @Test
    public void when_constructWithProcessorSupplier_then_suppliesCorrectProcessor() throws Exception {
        // When
        v = new Vertex("v", ProcessorSupplier.of(NoopP::new));

        // Then
        validateProcessor();
    }

    @Test
    public void when_constructWithMetaSupplier_then_suppliesCorrectProcessor() throws Exception {
        // When
        v = new Vertex("v", ProcessorMetaSupplier.of(NoopP::new));

        // Then
        validateProcessor();
    }

    @Test
    public void when_setLocalParallelism_then_hasThatParallelism() {
        // Given
        v = new Vertex("v", NoopP::new);

        // When
        v.localParallelism(1);

        // Then
        assertEquals(1, v.getLocalParallelism());
    }

    @Test
    public void when_badLocalParallelism_then_error() {
        v = new Vertex("v", NoopP::new);
        v.localParallelism(4);
        v.localParallelism(-1); // this is good

        exception.expect(IllegalArgumentException.class);
        v.localParallelism(-5); // this is not good
    }

    private void validateProcessor() throws UnknownHostException {
        Address address = new Address("localhost", 5701);
        Function<? super Address, ? extends ProcessorSupplier> fn = v.getMetaSupplier().get(singletonList(address));
        ProcessorSupplier supplier = fn.apply(address);
        Collection<? extends Processor> processors = supplier.get(1);
        assertEquals(NoopP.class, processors.iterator().next().getClass());
    }

    private static class NoopP extends AbstractProcessor {
    }
}
