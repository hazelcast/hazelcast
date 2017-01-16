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

import com.hazelcast.internal.serialization.impl.ByteArrayObjectDataOutputTest;
import com.hazelcast.jet.Processors.NoopProducer;
import com.hazelcast.nio.Address;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

@Category(QuickTest.class)
public class VertexTest {
    private Vertex v;

    @Test
    public void when_emptyConstructor_then_initialValues() {
        // When
        v = new Vertex();

        // Then
        assertNull(v.getSupplier());
        assertNull(v.getName());
        assertEquals(-1, v.getLocalParallelism());
    }

    @Test
    public void when_constructWithName_then_hasThatName() {
        // When
        v = new Vertex("v", NoopProducer::new);

        // Then
        assertEquals("v", v.getName());
    }

    @Test
    public void when_constructWithSimpleSupplier_then_suppliesCorrectProcessor() {
        // When
        v = new Vertex("v", NoopProducer::new);

        // Then
        validateProcessor();
    }

    @Test
    public void when_constructWithProcessorSupplier_then_suppliesCorrectProcessor() {
        // When
        v = new Vertex("v", ProcessorSupplier.of(NoopProducer::new));

        // Then
        validateProcessor();
    }

    @Test
    public void when_constructWithMetaSupplier_then_suppliesCorrectProcessor() {
        // When
        v = new Vertex("v", ProcessorMetaSupplier.of(NoopProducer::new));

        // Then
        validateProcessor();
    }

    @Test
    public void when_setLocalParallelism_then_hasThatParallelism() {
        // Given
        v = new Vertex("v", NoopProducer::new);

        // When
        v.localParallelism(1);

        // Then
        assertEquals(1, v.getLocalParallelism());
    }

    private void validateProcessor() {
        assertEquals(NoopProducer.class, v.getSupplier().get(new Address()).get(1).get(0).getClass());
    }
}
