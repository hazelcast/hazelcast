/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl.index;

import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.exec.scan.index.IndexEqualsFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexInFilter;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertSame;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IndexInFilterTest extends IndexFilterTestSupport {
    @Test
    public void testContent() {
        List<IndexFilter> filters = Collections.singletonList(new IndexEqualsFilter(intValue(1)));

        IndexInFilter filter = new IndexInFilter(filters);

        assertSame(filters, filter.getFilters());
    }

    @Test
    public void testEquals() {
        IndexInFilter filter = new IndexInFilter(Collections.singletonList(new IndexEqualsFilter(intValue(1))));

        checkEquals(filter, new IndexInFilter(Collections.singletonList(new IndexEqualsFilter(intValue(1)))), true);
        checkEquals(filter, new IndexInFilter(Collections.singletonList(new IndexEqualsFilter(intValue(2)))), false);
    }

    @Test
    public void testSerialization() {
        IndexInFilter original = new IndexInFilter(Collections.singletonList(new IndexEqualsFilter(intValue(1))));
        IndexInFilter restored = serializeAndCheck(original, SqlDataSerializerHook.INDEX_FILTER_IN);

        checkEquals(original, restored, true);
    }
}
