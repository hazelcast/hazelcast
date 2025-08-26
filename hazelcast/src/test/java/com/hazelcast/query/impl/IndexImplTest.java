/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.internal.monitor.impl.MemberPartitionStateImpl;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.monitor.impl.PerIndexStats;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IndexImplTest {

    private static final String ATTRIBUTE_NAME = "attribute";

    private IndexImpl index;

    private InternalSerializationService mockSerializationService;
    private Extractors mockExtractors;

    @Before
    public void setUp() {
        mockSerializationService = mock(InternalSerializationService.class);
        mockExtractors = Extractors.newBuilder(mockSerializationService).build();

        IndexConfig config = IndexUtils.createTestIndexConfig(IndexType.HASH, ATTRIBUTE_NAME);

        index = new IndexImpl(
            null,
            config,
            mockSerializationService,
            mockExtractors,
            IndexCopyBehavior.COPY_ON_READ,
            PerIndexStats.EMPTY,
            MemberPartitionStateImpl.DEFAULT_PARTITION_COUNT,
            "test"
        );
    }

    /**
     * Assert {@link CachedQueryEntry#getKey()} <em>isn't</em> called (i.e. the key isn't deserialized) when an entry is added,
     * thanks to the {@link #index}
     */
    @Test
    public void saveEntryIndex_doNotDeserializeKey() {
        CachedQueryEntry<Object, Object> entry = createQueryableEntry();
        index.putEntry(entry, null, entry, Index.OperationSource.USER);
    }

    private CachedQueryEntry<Object, Object> createQueryableEntry() {
        return new CachedQueryEntry<>(mockSerializationService, mock(Data.class), null, mockExtractors) {
            @Override
            public Object getKey() {
                fail("\"getKey()\" should not have been called");
                return null;
            }
        };
    }
}
