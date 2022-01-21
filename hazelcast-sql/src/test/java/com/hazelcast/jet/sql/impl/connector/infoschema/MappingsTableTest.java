/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.connector.infoschema;

import com.hazelcast.sql.impl.schema.Mapping;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MappingsTableTest {

    @Test
    public void test_rows() {
        // given
        Mapping mapping = new Mapping(
                "table-name",
                "table-external-name",
                "table-type",
                emptyList(),
                singletonMap("key", "value")
        );

        MappingsTable mappingTable = new MappingsTable("catalog", null, "table-schema", singletonList(mapping));

        // when
        List<Object[]> rows = mappingTable.rows();

        // then
        assertThat(rows).containsExactly(new Object[]{
                "catalog"
                , "table-schema"
                , "table-name"
                , "table-external-name"
                , "table-type"
                , "{\"key\":\"value\"}"
        });
    }
}
