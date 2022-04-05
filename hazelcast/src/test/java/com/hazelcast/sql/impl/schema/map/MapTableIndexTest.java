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

package com.hazelcast.sql.impl.schema.map;

import com.hazelcast.config.IndexType;
import com.hazelcast.sql.impl.CoreSqlTestSupport;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static com.hazelcast.config.IndexType.HASH;
import static com.hazelcast.config.IndexType.SORTED;
import static com.hazelcast.sql.impl.type.QueryDataType.BIGINT;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapTableIndexTest extends CoreSqlTestSupport {
    @Test
    public void testContent() {
        MapTableIndex index = new MapTableIndex("index", SORTED, 1, singletonList(1), singletonList(INT));

        assertEquals("index", index.getName());
        assertEquals(SORTED, index.getType());
        assertEquals(1, index.getComponentsCount());
        assertEquals(singletonList(1), index.getFieldOrdinals());
        assertEquals(singletonList(INT), index.getFieldConverterTypes());
    }

    @Test
    public void testEquals() {
        String name1 = "index1";
        String name2 = "index2";

        IndexType type1 = SORTED;
        IndexType type2 = HASH;

        int components1 = 1;
        int components2 = 2;

        List<Integer> ordinals1 = singletonList(1);
        List<Integer> ordinals2 = singletonList(2);

        List<QueryDataType> types1 = singletonList(INT);
        List<QueryDataType> types2 = singletonList(BIGINT);

        MapTableIndex index = new MapTableIndex(name1, type1, components1, ordinals1, types1);

        checkEquals(index, new MapTableIndex(name1, type1, components1, ordinals1, types1), true);

        checkEquals(index, new MapTableIndex(name2, type1, components1, ordinals1, types1), false);
        checkEquals(index, new MapTableIndex(name1, type2, components1, ordinals1, types1), false);
        checkEquals(index, new MapTableIndex(name1, type1, components2, ordinals1, types1), false);
        checkEquals(index, new MapTableIndex(name1, type1, components1, ordinals2, types1), false);
        checkEquals(index, new MapTableIndex(name1, type1, components1, ordinals1, types2), false);
    }
}
