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
import com.hazelcast.config.IndexType;
import com.hazelcast.jet.sql.impl.calcite.parser.HazelcastSqlParser;
import com.hazelcast.sql.impl.type.QueryDataType;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParser.Config;
import org.junit.Test;
import org.junit.runner.RunWith;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(JUnitParamsRunner.class)
public class HazelcastSqlParserTest {

    private static final Config CONFIG = SqlParser.configBuilder()
            .setCaseSensitive(true)
            .setUnquotedCasing(Casing.UNCHANGED)
            .setQuotedCasing(Casing.UNCHANGED)
            .setQuoting(Quoting.DOUBLE_QUOTE)
            .setParserFactory(HazelcastSqlParser.FACTORY)
            .build();

    @Test
    @Parameters({
            "true, false",
            "false, true"
    })
    public void test_createMapping(boolean replace, boolean ifNotExists) throws SqlParseException {
        // given
        String sql = "CREATE "
                + (replace ? "OR REPLACE " : "")
                + "MAPPING "
                + (ifNotExists ? "IF NOT EXISTS " : "")
                + "mapping_name ("
                + "column_name INT EXTERNAL NAME \"external.column.name\""
                + ")"
                + "TYPE mapping_type "
                + "OPTIONS("
                + "'option.key'='option.value'"
                + ")";

        // when
        SqlCreateMapping node = (SqlCreateMapping) parse(sql);

        // then
        assertThat(node.nameWithoutSchema()).isEqualTo("mapping_name");
        assertThat(node.externalName()).isEqualTo("mapping_name");
        assertThat(node.type()).isEqualTo("mapping_type");
        assertThat(node.columns().findFirst())
                .isNotEmpty().get()
                .extracting(column -> new Object[]{column.name(), column.type(), column.externalName()})
                .isEqualTo(new Object[]{"column_name", QueryDataType.INT, "external.column.name"});
        assertThat(node.options()).isEqualTo(ImmutableMap.of("option.key", "option.value"));
        assertThat(node.getReplace()).isEqualTo(replace);
        assertThat(node.ifNotExists()).isEqualTo(ifNotExists);
    }

    @Test
    @Parameters({
            "true",
            "false"
    })
    public void test_createIndex(boolean ifNotExists) throws SqlParseException {
        // given
        String sql = "CREATE INDEX "
                + (ifNotExists ? "IF NOT EXISTS " : "")
                + "index_name "
                + "ON mapping_name "
                + "(column_name1, column_name2) "
                + "TYPE SORTED "
                + "OPTIONS ("
                + "'option.key'='option.value'"
                + ")";

        // when
        SqlNode parsedNode = parse(sql);

        // then
        assertThat(parsedNode).isInstanceOf(SqlCreateIndex.class);

        // when
        SqlCreateIndex node = (SqlCreateIndex) parsedNode;

        // then
        assertThat(node.mapName()).isEqualTo("mapping_name");
        assertThat(node.indexName()).isEqualTo("index_name");
        assertThat(node.type()).isEqualTo(IndexType.SORTED);
        assertThat(node.columns().get(0)).isEqualTo("column_name1");
        assertThat(node.options()).isEqualTo(ImmutableMap.of("option.key", "option.value"));
        assertThat(node.ifNotExists()).isEqualTo(ifNotExists);
    }

    @Test
    @Parameters({
            "true",
            "false"
    })
    public void test_createIndexWithoutOptions(boolean ifNotExists) throws SqlParseException {
        // given
        String sql = "CREATE INDEX "
                + (ifNotExists ? "IF NOT EXISTS " : "")
                + "index_name "
                + "ON map_name "
                + "(column_name1, column_name2) "
                + "TYPE SORTED ";

        // when
        SqlNode parsedNode = parse(sql);

        // then
        assertThat(parsedNode).isInstanceOf(SqlCreateIndex.class);

        // when
        SqlCreateIndex node = (SqlCreateIndex) parsedNode;

        // then
        assertThat(node.mapName()).isEqualTo("map_name");
        assertThat(node.indexName()).isEqualTo("index_name");
        assertThat(node.type()).isEqualTo(IndexType.SORTED);
        assertThat(node.columns().get(0)).isEqualTo("column_name1");
        assertThat(node.ifNotExists()).isEqualTo(ifNotExists);
    }

    @Test
    @Parameters({
            "true",
            "false"
    })
    public void test_createIndexRequiresMapName(boolean ifNotExists) {
        // given
        String sql = "CREATE INDEX "
                + (ifNotExists ? "IF NOT EXISTS " : "")
                + "index_name ";

        // when & then
        assertThatThrownBy(() -> parse(sql))
                .hasMessageContaining("Encountered \"<EOF>\" at line 1")
                .hasMessageContaining("\"ON\" ...");
    }

    @Test
    public void test_createExternalMapping() throws SqlParseException {
        // given
        String sql = "CREATE EXTERNAL MAPPING "
                + "mapping_name "
                + "TYPE mapping_type";

        // when
        SqlNode node = parse(sql);

        // then
        assertThat(node).isInstanceOf(SqlCreateMapping.class);
    }

    @Test
    public void test_createMappingWithExternalName() throws SqlParseException {
        // given
        String sql = "CREATE MAPPING "
                + "mapping_name EXTERNAL NAME \"external.mapping.name\""
                + "TYPE mapping_type";

        // when
        SqlCreateMapping node = (SqlCreateMapping) parse(sql);

        // then
        assertThat(node.nameWithoutSchema()).isEqualTo("mapping_name");
        assertThat(node.externalName()).isEqualTo("external.mapping.name");
    }

    @Test
    public void test_createMappingWithSchema() throws SqlParseException {
        // given
        String sql = "CREATE MAPPING "
                + "public.mapping_name "
                + "TYPE mapping_type";

        // when
        SqlCreateMapping node = (SqlCreateMapping) parse(sql);

        // then
        assertThat(node.nameWithoutSchema()).isEqualTo("mapping_name");
        assertThat(node.externalName()).isEqualTo("mapping_name");
    }

    @Test
    public void test_createMappingRequiresColumns() {
        // given
        String sql = "CREATE MAPPING "
                + "mapping_name ("
                + ")"
                + "TYPE mapping_type";

        // when
        assertThatThrownBy(() -> parse(sql))
                .hasMessageContaining("Encountered \")\" at line 1, column 30");
    }

    @Test
    public void test_createMappingRequiresColumnType() {
        // given
        String sql = "CREATE MAPPING "
                + "mapping_name ("
                + "column_name"
                + ")"
                + "TYPE mapping_type";

        // when
        assertThatThrownBy(() -> parse(sql))
                .hasMessageContaining("Encountered \")\" at line 1, column 41");
    }

    @Test
    public void test_createMappingRequiresMappingType() {
        // given
        String sql = "CREATE MAPPING "
                + "mapping_name ("
                + "column_name INT"
                + ")";

        // when
        assertThatThrownBy(() -> parse(sql))
                .hasMessageContaining("Encountered \"<EOF>\" at line 1, column 45");
    }

    @Test
    public void test_createMappingRequiresMappingTypeSimpleIdentifier() {
        // given
        String sql = "CREATE MAPPING "
                + "mapping_name "
                + "TYPE mapping.type";

        // when
        assertThatThrownBy(() -> parse(sql))
                .hasMessageContaining("Encountered \".\" at line 1, column 41");
    }

    @Test
    @Parameters({
            "false",
            "true"
    })
    public void test_dropMapping(boolean ifExists) throws SqlParseException {
        // given
        String sql = "DROP MAPPING " + (ifExists ? "IF EXISTS " : "") + "mapping_name";

        // when
        SqlDropMapping node = (SqlDropMapping) parse(sql);

        // then
        assertThat(node.nameWithoutSchema()).isEqualTo("mapping_name");
        assertThat(node.ifExists()).isEqualTo(ifExists);
    }

    @Test
    @Parameters({
            "false",
            "true"
    })
    public void test_dropIndex(boolean ifExists) throws SqlParseException {
        // given
        String sql = "DROP INDEX " + (ifExists ? "IF EXISTS " : "") + "index_name ON object_name";

        // when
        SqlDropIndex node = (SqlDropIndex) parse(sql);

        // then
        assertThat(node.indexName()).isEqualTo("index_name");
        assertThat(node.objectName()).isEqualTo("object_name");
        assertThat(node.ifExists()).isEqualTo(ifExists);
    }

    @Test
    public void test_dropExternalMapping() throws SqlParseException {
        // given
        String sql = "DROP EXTERNAL MAPPING mapping_name";

        // when
        SqlNode node = parse(sql);

        // then
        assertThat(node).isInstanceOf(SqlDropMapping.class);
    }

    @Test
    public void test_dropExternalMappingWithSchema() throws SqlParseException {
        // given
        String sql = "DROP MAPPING some_schema.mapping_name";

        // when
        SqlDropMapping node = (SqlDropMapping) parse(sql);

        // then
        assertThat(node.nameWithoutSchema()).isEqualTo("mapping_name");
    }

    @Test
    @Parameters({
            "true",
            "false"
    })
    public void test_createView(boolean toReplace) throws SqlParseException {
        // given
        String sql = "CREATE  "
                + (toReplace ? " OR REPLACE " : "")
                + "VIEW "
                + "view_name "
                + "AS "
                + "SELECT * FROM map";

        // when
        SqlNode parsedNode = parse(sql);

        // then
        assertThat(parsedNode).isInstanceOf(SqlCreateView.class);

        // when
        SqlCreateView node = (SqlCreateView) parsedNode;

        // then
        assertThat(node.name()).isEqualTo("view_name");
        assertThat(node.getQuery()).isInstanceOf(SqlSelect.class);
        assertThat(node.getReplace()).isEqualTo(toReplace);
    }

    @Test
    @Parameters({
            "true",
            "false"
    })
    public void test_createViewRequiresQuery(boolean toReplace) throws SqlParseException {
        // given
        String sql = "CREATE  "
                + (toReplace ? " OR REPLACE " : "")
                + "VIEW "
                + "view_name ";

        // then
        assertThatThrownBy(() -> parse(sql))
                .hasMessageContaining("Encountered \"<EOF>\" at line 1");
    }

    @Test
    @Parameters({
            "true",
            "false"
    })
    public void test_dropView(boolean ifExists) throws SqlParseException {
        // given
        String sql = "DROP  "
                + "VIEW "
                + (ifExists ? " IF EXISTS " : "")
                + "view_name ";

        // when
        SqlNode parsedNode = parse(sql);

        // then
        assertThat(parsedNode).isInstanceOf(SqlDropView.class);

        // when
        SqlDropView node = (SqlDropView) parsedNode;

        // then
        assertThat(node.viewName()).isEqualTo("view_name");
        assertThat(node.ifExists()).isEqualTo(ifExists);
    }

    @Test
    public void test_sinkIntoWithValues() throws SqlParseException {
        // given
        String sql = "SINK INTO partitioned.t (id, name) VALUES (1, '1')";

        // when
        SqlExtendedInsert node = (SqlExtendedInsert) parse(sql);

        // then
        assertThat(node.tableNames()).isEqualTo(asList("partitioned", "t"));
        assertThat(node.getSource().getKind()).isEqualTo(SqlKind.VALUES);
        assertThat(node.getTargetColumnList()).hasSize(2);
    }

    @Test
    public void test_sinkIntoWithSelect() throws SqlParseException {
        // given
        String sql = "SINK INTO t SELECT * FROM s";

        // when
        SqlExtendedInsert node = (SqlExtendedInsert) parse(sql);

        // then
        assertThat(node.tableNames()).isEqualTo(singletonList("t"));
        assertThat(node.getSource().getKind()).isEqualTo(SqlKind.SELECT);
        assertThat(node.getTargetColumnList()).isNullOrEmpty();
    }

    @Test
    public void test_insertIntoWithValues() throws SqlParseException {
        // given
        String sql = "INSERT INTO partitioned.t (id, name) VALUES (1, '1')";

        // when
        SqlExtendedInsert node = (SqlExtendedInsert) parse(sql);

        // then
        assertThat(node.tableNames()).isEqualTo(asList("partitioned", "t"));
        assertThat(node.getSource().getKind()).isEqualTo(SqlKind.VALUES);
        assertThat(node.getTargetColumnList()).hasSize(2);
    }

    @Test
    public void test_insertIntoWithSelect() throws SqlParseException {
        // given
        String sql = "INSERT INTO t SELECT * FROM s";

        // when
        SqlExtendedInsert node = (SqlExtendedInsert) parse(sql);

        // then
        assertThat(node.tableNames()).isEqualTo(singletonList("t"));
        assertThat(node.getSource().getKind()).isEqualTo(SqlKind.SELECT);
        assertThat(node.getTargetColumnList()).isNullOrEmpty();
    }

    private static SqlNode parse(String sql) throws SqlParseException {
        return SqlParser.create(sql, CONFIG).parseStmt();
    }
}
