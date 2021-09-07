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

package com.hazelcast.jet.sql.impl.schema;

import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.schema.model.Person;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.schema.Mapping;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

public class SqlMappingTest extends SqlTestSupport {

    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() {
        initialize(1, null);
        sqlService = instance().getSql();
    }

    @Test
    public void when_mappingIsNotCreated_then_itIsNotAvailable() {
        assertThatThrownBy(() -> sqlService.execute("SELECT * FROM map"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Object 'map' not found, did you forget to CREATE MAPPING?");
        assertThatThrownBy(() -> sqlService.execute("SELECT * FROM public.map"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Object 'map' not found within 'hazelcast.public', did you forget to CREATE MAPPING?");
        assertThatThrownBy(() -> sqlService.execute("SELECT * FROM hazelcast.public.map"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Object 'map' not found within 'hazelcast.public', did you forget to CREATE MAPPING?");
    }

    @Test
    public void when_mappingIsDeclared_then_itIsAvailable() {
        // given
        String name = randomName();

        createMapping(name, Integer.class, String.class);

        // when
        SqlResult queryResult = sqlService.execute("SELECT __key, this FROM public." + name);

        // then
        assertThat(queryResult.updateCount()).isEqualTo(-1);
        assertThat(queryResult.iterator()).isExhausted();
    }

    @Test
    public void when_mappingIsDropped_then_itIsNotAvailable() {
        // given
        String name = randomName();

        createMapping(name, Integer.class, Person.class);

        // when
        SqlResult dropResult = sqlService.execute("DROP MAPPING " + name);

        // then
        assertThat(dropResult.updateCount()).isEqualTo(0);
        assertThatThrownBy(() -> sqlService.execute("SELECT * FROM public." + name))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Object '" + name + "' not found within 'hazelcast.public'");
    }

    @Test
    public void when_orReplaceAndIfNotExistsUsedTogether_then_fail() {
        assertThatThrownBy(() -> sqlService.execute("CREATE OR REPLACE MAPPING IF NOT EXISTS t TYPE TestBatch"))
                .hasMessageContaining("OR REPLACE in conjunction with IF NOT EXISTS not supported");
    }

    @Test
    public void when_duplicateColumn_then_fail() {
        assertThatThrownBy(() -> sqlService.execute("CREATE MAPPING t (a INT, a BIGINT) TYPE TestBatch"))
                .hasMessageContaining("Column 'a' specified more than once");
    }

    @Test
    public void when_emptyColumnList_then_fail() {
        assertThatThrownBy(() -> sqlService.execute("create mapping t () type TestBatch"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageStartingWith("Encountered \")\" at line 1, column 19.");
    }

    @Test
    public void when_badType_then_fail() {
        assertThatThrownBy(() -> sqlService.execute("CREATE MAPPING m TYPE TooBad"))
                .hasMessageContaining("Unknown connector type: TooBad");
    }

    @Test
    public void test_alias_int_integer() {
        test_alias(Integer.class.getName(), "int", "integer");
    }

    @Test
    public void test_alias_double_doublePrecision() {
        test_alias(Double.class.getName(), "double", "double precision");
    }

    @Test
    public void test_alias_dec_decimal_numeric() {
        test_alias(BigDecimal.class.getName(), "dec", "decimal", "numeric");
    }

    @Test
    public void test_alias_varchar_charVarying_characterVarying() {
        test_alias(String.class.getName(), "varchar", "char varying", "character varying");
    }

    private void test_alias(String javaClassName, String... aliases) {
        for (String alias : aliases) {
            sqlService.execute("CREATE MAPPING \"m_" + alias + "\"(__key " + alias + ") TYPE IMap " +
                    "OPTIONS('keyFormat'='java', 'keyJavaClass'='" + javaClassName + "', 'valueFormat'='json-flat')");
        }

        MappingStorage mappingStorage = new MappingStorage(getNodeEngineImpl(instance()));
        assertEquals(aliases.length, mappingStorage.values().size());
        Iterator<Mapping> iterator = mappingStorage.values().iterator();

        // the two mappings must be equal, except for their name & objectName
        Mapping firstMapping = iterator.next();
        while (iterator.hasNext()) {
            assertThat(iterator.next()).isEqualToIgnoringGivenFields(firstMapping, "name", "externalName");
        }
    }

    @Test
    public void test_caseInsensitiveType() {
        sqlService.execute("CREATE MAPPING t1 TYPE AllTypes");
        sqlService.execute("CREATE MAPPING t2 TYPE alltypes");
        sqlService.execute("CREATE MAPPING t3 TYPE ALLTYPES");
        sqlService.execute("CREATE MAPPING t4 TYPE aLlTyPeS");
    }

    @Test
    public void when_duplicateOption_then_fail() {
        assertThatThrownBy(() -> sqlService.execute("CREATE MAPPING t TYPE TestBatch OPTIONS('o'='1', 'o'='2')"))
                .hasMessageContaining("Option 'o' specified more than once");
    }

    @Test
    public void test_createDropMappingUsingSchema_public() {
        test_createDropMappingUsingSchema("public");
    }

    @Test
    public void test_createDropMappingUsingSchema_hazelcastPublic() {
        test_createDropMappingUsingSchema("hazelcast.public");
    }

    private void test_createDropMappingUsingSchema(String schemaName) {
        String name = randomName();
        createMapping(schemaName + "." + name, Integer.class, Integer.class);
        sqlService.execute("sink into " + name + " values(1, 1)");
        sqlService.execute("sink into public." + name + " values(2, 2)");
        sqlService.execute("sink into hazelcast.public." + name + " values(3, 3)");
        List<Row> expectedRows = asList(new Row(1, 1), new Row(2, 2), new Row(3, 3));
        assertRowsAnyOrder("select * from " + name, expectedRows);
        assertRowsAnyOrder("select * from public." + name, expectedRows);
        assertRowsAnyOrder("select * from hazelcast.public." + name, expectedRows);
        sqlService.execute("drop mapping " + schemaName + "." + name);
    }

    @Test
    public void when_createMappingInWrongSchema_then_fail() {
        assertThatThrownBy(() -> sqlService.execute("CREATE MAPPING badSchema.mapping TYPE TestBatch"))
                .hasMessage("From line 1, column 16 to line 1, column 32: " +
                        "The mapping must be created in the \"public\" schema");
        assertThatThrownBy(() -> sqlService.execute("CREATE MAPPING badSchema.public.mapping TYPE TestBatch"))
                .hasMessage("From line 1, column 16 to line 1, column 39: " +
                        "The mapping must be created in the \"public\" schema");
        assertThatThrownBy(() -> sqlService.execute("CREATE MAPPING hazelcast.badSchema.mapping TYPE TestBatch"))
                .hasMessage("From line 1, column 16 to line 1, column 42: " +
                        "The mapping must be created in the \"public\" schema");
    }

    @Test
    public void when_createMappingWithParameters_then_fail() {
        assertThatThrownBy(() -> sqlService.execute("CREATE MAPPING m TYPE TestBatch", "param"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessage("CREATE MAPPING does not support dynamic parameters");
    }

    @Test
    public void when_dropMappingWithParameters_then_fail() {
        assertThatThrownBy(() -> sqlService.execute("DROP MAPPING m", "param"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessage("DROP MAPPING does not support dynamic parameters");
    }
}
