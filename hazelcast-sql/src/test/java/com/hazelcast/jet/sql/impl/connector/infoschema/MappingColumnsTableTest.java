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
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.schema.view.View;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MappingColumnsTableTest {

    @Test
    public void test_rows() {
        // given
        Mapping mapping = new Mapping(
                "table-name",
                "table-external-name",
                "table-type",
                singletonList(new MappingField("table-field-name", INT, "table-field-external-name")),
                emptyMap());
        View view = new View("view-name", "select * from table-name", singletonList("col1"), singletonList(INT));
        MappingColumnsTable mappingColumnsTable =
                new MappingColumnsTable("catalog", null, "schema", singletonList(mapping), singletonList(view));

        // when
        List<Object[]> rows = mappingColumnsTable.rows();

        // then
        assertThat(rows).containsExactlyInAnyOrder(
                new Object[]{
                        "catalog",
                        "schema",
                        "table-name",
                        "table-field-name",
                        "table-field-external-name",
                        1,
                        "true",
                        "INTEGER"
                },
                new Object[]{
                        "catalog",
                        "schema",
                        "view-name",
                        "col1",
                        null,
                        1,
                        "true",
                        "INTEGER"
                }
        );
    }
}
