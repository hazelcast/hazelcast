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

package com.hazelcast.jet.sql.impl.connector.infoschema;

import com.hazelcast.jet.sql.impl.schema.Mapping;
import com.hazelcast.jet.sql.impl.schema.MappingField;
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
                null,
                emptyMap()
        );
        MappingColumnsTable mappingColumnsTable =
                new MappingColumnsTable("catalog", null, "table-schema", singletonList(mapping));

        // when
        List<Object[]> rows = mappingColumnsTable.rows();

        // then
        assertThat(rows).containsExactly(
                new Object[]{
                        "catalog",
                        "table-schema",
                        "table-name",
                        "table-field-name",
                        "table-field-external-name",
                        1,
                        "true",
                        "INTEGER"
                }
        );
    }
}
