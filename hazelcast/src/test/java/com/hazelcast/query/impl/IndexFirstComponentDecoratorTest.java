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

package com.hazelcast.query.impl;

import com.hazelcast.config.IndexType;
import com.hazelcast.internal.monitor.impl.MemberPartitionStateImpl;
import com.hazelcast.internal.monitor.impl.PerIndexStats;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IndexFirstComponentDecoratorTest {

    private InternalSerializationService serializationService;
    Extractors extractors;
    private InternalIndex expected;
    private InternalIndex actual;

    @Before
    public void before() {
        serializationService = new DefaultSerializationServiceBuilder().build();
        extractors = Extractors.newBuilder(serializationService).build();
        expected = new IndexImpl(
            IndexUtils.createTestIndexConfig(IndexType.SORTED, "this"),
            serializationService,
            extractors,
            IndexCopyBehavior.COPY_ON_READ,
            PerIndexStats.EMPTY,
            MemberPartitionStateImpl.DEFAULT_PARTITION_COUNT
        );

        InternalIndex compositeIndex = new IndexImpl(
            IndexUtils.createTestIndexConfig(IndexType.SORTED, "this", "__key"),
            serializationService,
            extractors,
            IndexCopyBehavior.COPY_ON_READ,
            PerIndexStats.EMPTY,
            MemberPartitionStateImpl.DEFAULT_PARTITION_COUNT
        );

        actual = new AttributeIndexRegistry.FirstComponentDecorator(compositeIndex);

        for (int i = 0; i < 100; ++i) {
            expected.putEntry(entry(i, i), null, entry(i, i), Index.OperationSource.USER);
            compositeIndex.putEntry(entry(i, i), null, entry(i, i), Index.OperationSource.USER);
        }
    }

    @Test
    public void testQuerying() {
        assertEquals(expected.getRecords(-1), actual.getRecords(-1));
        assertEquals(expected.getRecords(0), actual.getRecords(0));
        assertEquals(expected.getRecords(50), actual.getRecords(50));
        assertEquals(expected.getRecords(99), actual.getRecords(99));
        assertEquals(expected.getRecords(100), actual.getRecords(100));

        assertEquals(expected.getRecords(new Comparable[]{}), actual.getRecords(new Comparable[]{}));
        assertEquals(expected.getRecords(new Comparable[]{50}), actual.getRecords(new Comparable[]{50}));
        assertEquals(expected.getRecords(new Comparable[]{-1}), actual.getRecords(new Comparable[]{-1}));
        assertEquals(expected.getRecords(new Comparable[]{100}), actual.getRecords(new Comparable[]{100}));
        assertEquals(expected.getRecords(new Comparable[]{10, 10}), actual.getRecords(new Comparable[]{10, 10}));
        assertEquals(expected.getRecords(new Comparable[]{20, -1, 100}), actual.getRecords(new Comparable[]{20, -1, 100}));
        assertEquals(expected.getRecords(new Comparable[]{-1, -2, -3}), actual.getRecords(new Comparable[]{-1, -2, -3}));
        assertEquals(expected.getRecords(new Comparable[]{100, 101, 102}), actual.getRecords(new Comparable[]{100, 101, 102}));
        assertEquals(expected.getRecords(new Comparable[]{10, 20, 30, 30}), actual.getRecords(new Comparable[]{10, 20, 30, 30}));

        assertEquals(expected.getRecords(Comparison.LESS, 50), actual.getRecords(Comparison.LESS, 50));
        assertEquals(expected.getRecords(Comparison.LESS, 99), actual.getRecords(Comparison.LESS, 99));
        assertEquals(expected.getRecords(Comparison.LESS, 100), actual.getRecords(Comparison.LESS, 100));
        assertEquals(expected.getRecords(Comparison.LESS, 0), actual.getRecords(Comparison.LESS, 0));
        assertEquals(expected.getRecords(Comparison.LESS, -1), actual.getRecords(Comparison.LESS, -1));

        assertEquals(expected.getRecords(Comparison.GREATER, 50), actual.getRecords(Comparison.GREATER, 50));
        assertEquals(expected.getRecords(Comparison.GREATER, 99), actual.getRecords(Comparison.GREATER, 99));
        assertEquals(expected.getRecords(Comparison.GREATER, 100), actual.getRecords(Comparison.GREATER, 100));
        assertEquals(expected.getRecords(Comparison.GREATER, 0), actual.getRecords(Comparison.GREATER, 0));
        assertEquals(expected.getRecords(Comparison.GREATER, -1), actual.getRecords(Comparison.GREATER, -1));

        assertEquals(expected.getRecords(Comparison.LESS_OR_EQUAL, 50), actual.getRecords(Comparison.LESS_OR_EQUAL, 50));
        assertEquals(expected.getRecords(Comparison.LESS_OR_EQUAL, 99), actual.getRecords(Comparison.LESS_OR_EQUAL, 99));
        assertEquals(expected.getRecords(Comparison.LESS_OR_EQUAL, 100), actual.getRecords(Comparison.LESS_OR_EQUAL, 100));
        assertEquals(expected.getRecords(Comparison.LESS_OR_EQUAL, 0), actual.getRecords(Comparison.LESS_OR_EQUAL, 0));
        assertEquals(expected.getRecords(Comparison.LESS_OR_EQUAL, -1), actual.getRecords(Comparison.LESS_OR_EQUAL, -1));

        assertEquals(expected.getRecords(Comparison.GREATER_OR_EQUAL, 50), actual.getRecords(Comparison.GREATER_OR_EQUAL, 50));
        assertEquals(expected.getRecords(Comparison.GREATER_OR_EQUAL, 99), actual.getRecords(Comparison.GREATER_OR_EQUAL, 99));
        assertEquals(expected.getRecords(Comparison.GREATER_OR_EQUAL, 100), actual.getRecords(Comparison.GREATER_OR_EQUAL, 100));
        assertEquals(expected.getRecords(Comparison.GREATER_OR_EQUAL, 0), actual.getRecords(Comparison.GREATER_OR_EQUAL, 0));
        assertEquals(expected.getRecords(Comparison.GREATER_OR_EQUAL, -1), actual.getRecords(Comparison.GREATER_OR_EQUAL, -1));

        assertEquals(expected.getRecords(0, false, 99, false), actual.getRecords(0, false, 99, false));
        assertEquals(expected.getRecords(0, true, 99, false), actual.getRecords(0, true, 99, false));
        assertEquals(expected.getRecords(0, false, 99, true), actual.getRecords(0, false, 99, true));
        assertEquals(expected.getRecords(0, true, 99, true), actual.getRecords(0, true, 99, true));

        assertEquals(expected.getRecords(-10, false, 99, false), actual.getRecords(-10, false, 99, false));
        assertEquals(expected.getRecords(-10, true, 99, false), actual.getRecords(-10, true, 99, false));
        assertEquals(expected.getRecords(-10, false, 99, true), actual.getRecords(-10, false, 99, true));
        assertEquals(expected.getRecords(-10, true, 99, true), actual.getRecords(-10, true, 99, true));

        assertEquals(expected.getRecords(10, false, 50, false), actual.getRecords(10, false, 50, false));
        assertEquals(expected.getRecords(10, true, 50, false), actual.getRecords(10, true, 50, false));
        assertEquals(expected.getRecords(10, false, 50, true), actual.getRecords(10, false, 50, true));
        assertEquals(expected.getRecords(10, true, 50, true), actual.getRecords(10, true, 50, true));

        assertEquals(expected.getRecords(90, false, 150, false), actual.getRecords(90, false, 150, false));
        assertEquals(expected.getRecords(90, true, 150, false), actual.getRecords(90, true, 150, false));
        assertEquals(expected.getRecords(90, false, 150, true), actual.getRecords(90, false, 150, true));
        assertEquals(expected.getRecords(90, true, 150, true), actual.getRecords(90, true, 150, true));

        assertEquals(expected.getRecords(-100, false, -10, false), actual.getRecords(-100, false, -10, false));
        assertEquals(expected.getRecords(-100, true, -10, false), actual.getRecords(-100, true, -10, false));
        assertEquals(expected.getRecords(-100, false, -10, true), actual.getRecords(-100, false, -10, true));
        assertEquals(expected.getRecords(-100, true, -10, true), actual.getRecords(-100, true, -10, true));

        assertEquals(expected.getRecords(110, false, 150, false), actual.getRecords(110, false, 150, false));
        assertEquals(expected.getRecords(110, true, 150, false), actual.getRecords(110, true, 150, false));
        assertEquals(expected.getRecords(110, false, 150, true), actual.getRecords(110, false, 150, true));
        assertEquals(expected.getRecords(110, true, 150, true), actual.getRecords(110, true, 150, true));

        assertEquals(expected.getRecords(-100, false, 200, false), actual.getRecords(-100, false, 200, false));
        assertEquals(expected.getRecords(-100, true, 200, false), actual.getRecords(-100, true, 200, false));
        assertEquals(expected.getRecords(-100, false, 200, true), actual.getRecords(-100, false, 200, true));
        assertEquals(expected.getRecords(-100, true, 200, true), actual.getRecords(-100, true, 200, true));
    }

    private CachedQueryEntry<?, ?> entry(Object key, Object value) {
        return new CachedQueryEntry<>(serializationService, serializationService.toData(key), value, extractors);
    }

}
