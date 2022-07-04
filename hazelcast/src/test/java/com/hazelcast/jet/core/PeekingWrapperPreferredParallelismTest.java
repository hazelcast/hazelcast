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

import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.core.processor.DiagnosticProcessors.peekInputP;
import static com.hazelcast.jet.core.processor.DiagnosticProcessors.peekOutputP;
import static com.hazelcast.jet.core.processor.DiagnosticProcessors.peekSnapshotP;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PeekingWrapperPreferredParallelismTest {

    @Test
    public void when_peekInput_then_propagatesPreferredParallelism() {
        // Given
        int preferredParallelism = 3;
        ProcessorMetaSupplier wrappedMetaSupplier = ProcessorMetaSupplier.of(preferredParallelism, Processors.noopP());

        // When
        ProcessorMetaSupplier peekingMetaSupplier = peekInputP(wrappedMetaSupplier);

        // Then
        assertEquals(preferredParallelism, peekingMetaSupplier.preferredLocalParallelism());
    }

    @Test
    public void when_peekOutput_then_propagatesPreferredParallelism() {
        // Given
        int preferredParallelism = 3;
        ProcessorMetaSupplier wrappedMetaSupplier = ProcessorMetaSupplier.of(preferredParallelism, Processors.noopP());

        // When
        ProcessorMetaSupplier peekingMetaSupplier = peekOutputP(wrappedMetaSupplier);

        // Then
        assertEquals(preferredParallelism, peekingMetaSupplier.preferredLocalParallelism());
    }

    @Test
    public void when_peekSnapshot_then_propagatesPreferredParallelism() {
        // Given
        int preferredParallelism = 3;
        ProcessorMetaSupplier wrappedMetaSupplier = ProcessorMetaSupplier.of(preferredParallelism, Processors.noopP());

        // When
        ProcessorMetaSupplier peekingMetaSupplier = peekSnapshotP(wrappedMetaSupplier);

        // Then
        assertEquals(preferredParallelism, peekingMetaSupplier.preferredLocalParallelism());
    }
}
