/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.mapstore;

import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlService;
import com.hazelcast.test.annotation.QuickTest;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
@Category(QuickTest.class)
public class MappingHelperTest {

    // SqlService#execute returns closeable SqlResult, RETURNS_DEEP_STUBS prevents NPE
    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private SqlService sqlService;

    private MappingHelper mappingHelper;

    @Before
    public void setUp() {
        mappingHelper = new MappingHelper(sqlService);
    }

    @Test
    public void when_createMapping_then_quoteParameters() {
        mappingHelper.createMapping(
                "myMapping",
                "myTable",
                singletonList(new SqlColumnMetadata("id", SqlColumnType.INTEGER, true)),
                "dataConnectionRef",
                "idColumn"
        );

        verify(sqlService).execute(
                "CREATE MAPPING \"myMapping\" " +
                        "EXTERNAL NAME \"myTable\" " +
                        "( \"id\" INTEGER ) " +
                        "DATA CONNECTION \"dataConnectionRef\" " +
                        "OPTIONS (" +
                        " 'idColumn' = 'idColumn' " +
                        ")"
        );
    }

    @Test
    public void when_createMappingWithTwoColumns_then_quoteParameters() {
        mappingHelper.createMapping(
                "myMapping",
                "myTable",
                asList(
                        new SqlColumnMetadata("id", SqlColumnType.INTEGER, true),
                        new SqlColumnMetadata("name", SqlColumnType.VARCHAR, true)
                ),
                "dataConnectionRef",
                "idColumn"
        );

        verify(sqlService).execute(
                "CREATE MAPPING \"myMapping\" " +
                        "EXTERNAL NAME \"myTable\" " +
                        "( \"id\" INTEGER, \"name\" VARCHAR ) " +
                        "DATA CONNECTION \"dataConnectionRef\" " +
                        "OPTIONS (" +
                        " 'idColumn' = 'idColumn' " +
                        ")"
        );
    }

    @Test
    public void when_createMapping_then_escapeParameters() {
        mappingHelper.createMapping(
                "my\"Mapping",
                "my\"Table",
                singletonList(new SqlColumnMetadata("id\"", SqlColumnType.INTEGER, true)),
                "data\"ConnectionRef",
                "id'Column"
        );

        verify(sqlService).execute(
                "CREATE MAPPING \"my\"\"Mapping\" " +
                        "EXTERNAL NAME \"my\"\"Table\" " +
                        "( \"id\"\"\" INTEGER ) " +
                        "DATA CONNECTION \"data\"\"ConnectionRef\" " +
                        "OPTIONS (" +
                        " 'idColumn' = 'id''Column' " +
                        ")"
        );
    }

    @Test
    public void when_dropMapping_then_quoteMappingName() {
        mappingHelper.dropMapping("myMapping");

        verify(sqlService).execute("DROP MAPPING IF EXISTS \"myMapping\"");
    }

    @Test
    public void when_dropMapping_then_escapeMappingName() {
        mappingHelper.dropMapping("my\"Mapping");

        verify(sqlService).execute("DROP MAPPING IF EXISTS \"my\"\"Mapping\"");
    }

    @Test
    public void when_loadColumnMetadataFromMapping_then_quoteMappingName() {
        mappingHelper.loadColumnMetadataFromMapping("myMapping");

        verify(sqlService).execute(
                "SELECT * FROM information_schema.columns "
                        + "WHERE table_name = ? ORDER BY ordinal_position ASC",
                "myMapping"
        );
    }

    @Test
    public void when_loadColumnMetadataFromMapping_then_escapeMappingName() {
        mappingHelper.loadColumnMetadataFromMapping("my\"Mapping");

        verify(sqlService).execute(
                "SELECT * FROM information_schema.columns "
                        + "WHERE table_name = ? ORDER BY ordinal_position ASC",
                "my\"Mapping"
        );
    }

    @Test
    public void externalName_simple() {
        String input = "my_table";
        String actual = MappingHelper.externalName(input);
        Assertions.assertThat(actual).isEqualTo("\"my_table\"");
    }

    @Test
    public void externalName_with_dot() {
        String input = "custom_schema.my_table";
        String actual = MappingHelper.externalName(input);
        Assertions.assertThat(actual).isEqualTo("\"custom_schema\".\"my_table\"");
    }

    @Test
    public void externalName_with_2dots() {
        String input = "catalog.custom_schema.my_table";
        String actual = MappingHelper.externalName(input);
        Assertions.assertThat(actual).isEqualTo("\"catalog\".\"custom_schema\".\"my_table\"");
    }

    @Test
    public void externalName_withQuotes() {
        String input = "custom_schema.\"my_table\"";
        String actual = MappingHelper.externalName(input);
        Assertions.assertThat(actual).isEqualTo("\"custom_schema\".\"my_table\"");
    }

    @Test
    public void externalName_with2Quotes() {
        String input = "\"custom_schema\".\"my_table\"";
        String actual = MappingHelper.externalName(input);
        Assertions.assertThat(actual).isEqualTo("\"custom_schema\".\"my_table\"");
    }

    @Test
    public void externalName_with3Quotes() {
        String input = "\"catalog\".\"custom_schema\".\"my_table\"";
        String actual = MappingHelper.externalName(input);
        Assertions.assertThat(actual).isEqualTo("\"catalog\".\"custom_schema\".\"my_table\"");
    }

    @Test
    public void externalName_with_quotes_and_dots() {
        String input = "custom_schema.\"table.with_dot\"";
        String actual = MappingHelper.externalName(input);
        Assertions.assertThat(actual).isEqualTo("\"custom_schema\".\"table.with_dot\"");
    }

    @Test
    public void externalName_with_backticks_and_dots() {
        String input = "custom_schema.`table.with_dot`";
        String actual = MappingHelper.externalName(input);
        Assertions.assertThat(actual).isEqualTo("\"custom_schema\".\"table.with_dot\"");
    }
}
