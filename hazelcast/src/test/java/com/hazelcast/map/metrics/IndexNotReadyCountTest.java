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

package com.hazelcast.map.metrics;

import com.hazelcast.config.IndexType;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.query.impl.IndexCopyBehavior;
import com.hazelcast.query.impl.IndexRegistry;
import com.hazelcast.query.impl.IndexUtils;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.query.impl.QueryContext.IndexMatchHint;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.jupiter.api.Test;

import static com.hazelcast.config.MapConfig.DEFAULT_IN_MEMORY_FORMAT;
import static org.assertj.core.api.Assertions.assertThat;

@ParallelJVMTest
@QuickTest
class IndexNotReadyCountTest {

    @Test
    void testCounterIncrementOnUnindexedPartitions() {
        IndexRegistry indexRegistry = IndexRegistry.newBuilder(null, "test", new DefaultSerializationServiceBuilder().build(),
                IndexCopyBehavior.NEVER, DEFAULT_IN_MEMORY_FORMAT).global(true).statsEnabled(true).build();

        InternalIndex index = indexRegistry.addOrGetIndex(IndexUtils.createTestIndexConfig(IndexType.HASH, "abc"));

        assertThat(indexRegistry.matchIndex("abc", IndexMatchHint.NONE, 0)).isSameAs(index);
        assertThat(index.getPerIndexStats().getIndexNotReadyQueryCount()).isZero();

        assertThat(indexRegistry.matchIndex("abc", IndexMatchHint.NONE, 1)).isNull();
        assertThat(index.getPerIndexStats().getIndexNotReadyQueryCount()).isEqualTo(1);
    }
}
