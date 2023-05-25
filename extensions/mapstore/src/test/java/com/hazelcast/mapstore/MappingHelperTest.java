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
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
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
    public void quoteExternalName_simple() {
        String externalName = "my_table";
        StringBuilder sb = new StringBuilder();
        MappingHelper.quoteExternalName(sb, externalName);
        assertThat(sb.toString()).isEqualTo("\"my_table\"");
    }

    @Test
    public void quoteExternalName_with_dot() {
        String externalName = "custom_schema.my_table";
        StringBuilder sb = new StringBuilder();
        MappingHelper.quoteExternalName(sb, externalName);
        assertThat(sb.toString()).isEqualTo("\"custom_schema\".\"my_table\"");
    }

    @Test
    public void quoteExternalName_with_hyphen() {
        String externalName = "schema-with-hyphen.table-with-hyphen";
        StringBuilder sb = new StringBuilder();
        MappingHelper.quoteExternalName(sb, externalName);
        assertThat(sb.toString()).isEqualTo("\"schema-with-hyphen\".\"table-with-hyphen\"");
    }

    @Test
    public void quoteExternalName_with_space() {
        String externalName = "schema with space.table with space";
        StringBuilder sb = new StringBuilder();
        MappingHelper.quoteExternalName(sb, externalName);
        assertThat(sb.toString()).isEqualTo("\"schema with space\".\"table with space\"");
    }

    @Test
    public void quoteExternalName_with_2dots() {
        String externalName = "catalog.custom_schema.my_table";
        StringBuilder sb = new StringBuilder();
        MappingHelper.quoteExternalName(sb, externalName);
        assertThat(sb.toString()).isEqualTo("\"catalog\".\"custom_schema\".\"my_table\"");
    }

    @Test
    public void quoteExternalName_withQuotes() {
        String externalName = "custom_schema.\"my_table\"";
        StringBuilder sb = new StringBuilder();
        MappingHelper.quoteExternalName(sb, externalName);
        assertThat(sb.toString()).isEqualTo("\"custom_schema\".\"my_table\"");
    }

    @Test
    public void quoteExternalName_withEscapedQuotes() {
        String externalName = "custom_\"\"schema.\"my_\"\"table\"";
        StringBuilder sb = new StringBuilder();
        MappingHelper.quoteExternalName(sb, externalName);
        assertThat(sb.toString()).isEqualTo("\"custom_\"\"schema\".\"my_\"\"table\"");
    }

    @Test
    public void quoteExternalName_withMoreEscapedQuotes() {
        String externalName = "custom_\"\"\"\"schema.\"my_\"\"table\"\"\"";
        StringBuilder sb = new StringBuilder();
        MappingHelper.quoteExternalName(sb, externalName);
        assertThat(sb.toString()).isEqualTo("\"custom_\"\"\"\"schema\".\"my_\"\"table\"\"\"");
    }

    @Test
    public void quoteExternalName_with2Quotes() {
        String externalName = "\"custom_schema\".\"my_table\"";
        StringBuilder sb = new StringBuilder();
        MappingHelper.quoteExternalName(sb, externalName);
        assertThat(sb.toString()).isEqualTo("\"custom_schema\".\"my_table\"");
    }

    @Test
    public void quoteExternalName_with3Quotes() {
        String externalName = "\"catalog\".\"custom_schema\".\"my_table\"";
        StringBuilder sb = new StringBuilder();
        MappingHelper.quoteExternalName(sb, externalName);
        assertThat(sb.toString()).isEqualTo("\"catalog\".\"custom_schema\".\"my_table\"");
    }

    @Test
    public void quoteExternalName_with_quotes_and_dots() {
        String externalName = "custom_schema.\"table.with_dot\"";
        StringBuilder sb = new StringBuilder();
        MappingHelper.quoteExternalName(sb, externalName);
        assertThat(sb.toString()).isEqualTo("\"custom_schema\".\"table.with_dot\"");
    }

    @Test
    public void quoteExternalName_with_backticks_and_dots() {
        String externalName = "custom_schema.`table.with_dot`";
        StringBuilder sb = new StringBuilder();
        MappingHelper.quoteExternalName(sb, externalName);
        assertThat(sb.toString()).isEqualTo("\"custom_schema\".\"`table\".\"with_dot`\"");
    }
}
