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

package com.hazelcast.jet.sql.impl.parse;

import com.google.common.collect.ImmutableMap;
import com.hazelcast.sql.impl.schema.Mapping;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

public class SqlCreateMappingTest {

    private static final String LE = System.lineSeparator();

    @Test
    public void test_unparse() {
        Mapping mapping = new Mapping(
                "name",
                "external-name",
                "Type",
                asList(
                        new MappingField("field1", QueryDataType.VARCHAR, "__key.field1"),
                        new MappingField("field2", QueryDataType.INT, "this.field2")
                ),
                ImmutableMap.of("key1", "value1", "key2", "value2")
        );

        String sql = SqlCreateMapping.unparse(mapping);
        assertThat(sql).isEqualTo("CREATE MAPPING \"name\" EXTERNAL NAME \"external-name\" (" + LE +
                "  \"field1\" VARCHAR EXTERNAL NAME \"__key.field1\"," + LE +
                "  \"field2\" INTEGER EXTERNAL NAME \"this.field2\"" + LE +
                ")" + LE +
                "TYPE Type" + LE +
                "OPTIONS (" + LE +
                "  'key1' = 'value1'," + LE +
                "  'key2' = 'value2'" + LE +
                ")"
        );
    }

    @Test
    public void test_unparse_quoting() {
        Mapping mapping = new Mapping(
                "na\"me",
                "external\"name",
                "Type",
                singletonList(new MappingField("fi\"eld", QueryDataType.VARCHAR, "__key\"field")),
                ImmutableMap.of("ke'y", "val'ue")
        );

        String sql = SqlCreateMapping.unparse(mapping);
        assertThat(sql).isEqualTo("CREATE MAPPING \"na\"\"me\" EXTERNAL NAME \"external\"\"name\" (" + LE +
                "  \"fi\"\"eld\" VARCHAR EXTERNAL NAME \"__key\"\"field\"" + LE +
                ")" + LE +
                "TYPE Type" + LE +
                "OPTIONS (" + LE +
                "  'ke''y' = 'val''ue'" + LE +
                ")"
        );
    }
}
