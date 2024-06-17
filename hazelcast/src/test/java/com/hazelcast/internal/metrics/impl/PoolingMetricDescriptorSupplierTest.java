/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.metrics.impl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PoolingMetricDescriptorSupplierTest {

    @Test
    public void testSuppliesMoreThanInitialCapacity() {
        PoolingMetricDescriptorSupplier supplier = new PoolingMetricDescriptorSupplier();
        for (int i = 0; i < 2 * PoolingMetricDescriptorSupplier.INITIAL_CAPACITY; i++) {
            supplier.get();
        }
        // we just need the loop not to fail
    }

    @Test
    public void testLastRecycledSuppliedNext() {
        PoolingMetricDescriptorSupplier supplier = new PoolingMetricDescriptorSupplier();
        MetricDescriptorImpl first = supplier.get();
        supplier.recycle(first);
        MetricDescriptorImpl second = supplier.get();

        assertSame(first, second);
    }

    @Test
    public void testSuppliesNullAfterReleaseAll() {
        PoolingMetricDescriptorSupplier supplier = new PoolingMetricDescriptorSupplier();
        MetricDescriptorImpl descriptor = supplier.get();
        assertNotNull(descriptor);
        supplier.recycle(descriptor);

        supplier.close();

        assertThrows(IllegalStateException.class, supplier::get);
    }
}
