/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.MapAttributeConfig;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class IndexImplTest {

    private static final String ATTRIBUTE_NAME = "attribute";

    private IndexImpl index;

    @Before
    public void setUp() {
        InternalSerializationService mockSerializationService = mock(InternalSerializationService.class);
        Extractors mockExtractors = new Extractors(Collections.<MapAttributeConfig>emptyList(), null);
        index = new IndexImpl(ATTRIBUTE_NAME, false, mockSerializationService, mockExtractors, IndexCopyBehavior.COPY_ON_READ);
    }

    @Test
    public void saveEntryIndex_doNotDeserializeKey() {
        QueryableEntry entry = createMockQueryableEntry();
        index.saveEntryIndex(entry, null);
        verify(entry, never()).getKey();
    }

    private QueryableEntry createMockQueryableEntry() {
        QueryableEntry entry = mock(QueryableEntry.class);
        Data keyData = mock(Data.class);
        when(entry.getKeyData()).thenReturn(keyData);
        return entry;
    }
}
