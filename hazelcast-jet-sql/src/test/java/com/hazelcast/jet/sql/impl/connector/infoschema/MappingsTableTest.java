/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.google.common.collect.ImmutableMap;
import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.jet.sql.impl.schema.MappingDefinition;
import com.hazelcast.sql.impl.schema.Table;
import org.junit.Test;

import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

public class MappingsTableTest {

    @Test
    @SuppressWarnings("ConstantConditions")
    public void test_rows() {
        // given
        Table table = new JetTable(null, emptyList(), "table-schema", "table-name", null);
        MappingDefinition definition = new MappingDefinition(table, "table-type", ImmutableMap.of("key", "value"));
        MappingsTable mappingTable = new MappingsTable("catalog", null, singletonList(definition));

        // when
        List<Object[]> rows = mappingTable.rows();

        // then
        assertThat(rows)
                .containsExactly(new Object[]{"catalog", "table-schema", "table-name", "table-type", "{key=value}"});
    }
}
