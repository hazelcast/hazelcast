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

package com.hazelcast.jet.sql.impl.schema;

import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.schema.model.IdentifiedPerson;
import com.hazelcast.jet.sql.impl.schema.model.Person;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlService;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.Map;

import static java.util.Collections.singletonList;
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
    public void when_mappingIsDeclared_then_itIsAvailable() {
        // given
        String name = randomName();

        // when
        SqlResult createResult = sqlService.execute(javaSerializableMapDdl(name, Integer.class, String.class));

        // then
        assertThat(createResult.updateCount()).isEqualTo(0);

        // when
        SqlResult queryResult = sqlService.execute("SELECT __key, this FROM public." + name);

        // then
        assertThat(queryResult.updateCount()).isEqualTo(-1);
        assertThat(queryResult.iterator()).isExhausted();
    }

    @Test
    public void when_mappingIsDeclared_then_itsDefinitionHasPrecedenceOverDiscoveredOne() {
        // given
        String name = randomName();

        sqlService.execute(javaSerializableMapDdl(name, Integer.class, Person.class));

        Map<Integer, Person> map = instance().getMap(name);
        map.put(1, new IdentifiedPerson(2, "Alice"));

        // when
        // then
        // The column `id` exists in IdentifiedPerson class (that was used in the sample entry), but not
        // in the Person class (that was used in the DDL). If the explicit mapping is used, we won't find it.
        assertThatThrownBy(() -> sqlService.execute("SELECT id FROM " + name))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Column 'id' not found in any table");
    }

    @Test
    public void when_mappingIsDropped_then_itIsNotAvailable() {
        // given
        String name = randomName();

        sqlService.execute(javaSerializableMapDdl(name, Integer.class, Person.class));

        // when
        SqlResult dropResult = sqlService.execute("DROP MAPPING " + name);

        // then
        assertThat(dropResult.updateCount()).isEqualTo(0);
        assertThatThrownBy(() -> sqlService.execute("SELECT * FROM public." + name))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Object '" + name + "' not found within 'hazelcast.public'");
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

    private void test_alias(String javaClassName, String ... aliases) {
        for (String alias : aliases) {
            sqlService.execute("CREATE MAPPING \"m_" + alias + "\"(__key " + alias + ") TYPE IMap " +
                    "OPTIONS(keyFormat 'java', keyJavaClass '" + javaClassName + "', valueFormat 'json')");
        }

        MappingStorage mappingStorage = new MappingStorage(getNodeEngineImpl(instance()));
        assertEquals(aliases.length, mappingStorage.values().size());
        Iterator<Mapping> iterator = mappingStorage.values().iterator();

        // the two mappings must be equal, except for their name
        Mapping firstMapping = iterator.next();
        while (iterator.hasNext()) {
            assertThat(iterator.next()).isEqualToIgnoringGivenFields(firstMapping, "name");
        }
    }

    @Test
    public void when_noFieldsResolved_then_wholeValueMapped() {
        String name = randomName();

        sqlService.execute(javaSerializableMapDdl(name, Object.class, Object.class));

        Person key = new Person("foo");
        Person value = new Person("bar");
        instance().getMap(name).put(key, value);

        assertRowsAnyOrder("SELECT __key, this FROM " + name,
                singletonList(new Row(key, value)));
    }

    @Test
    public void test_caseInsensitiveType() {
        sqlService.execute("CREATE MAPPING t1 TYPE TestStream");
        sqlService.execute("CREATE MAPPING t2 TYPE teststream");
        sqlService.execute("CREATE MAPPING t3 TYPE TESTSTREAM");
        sqlService.execute("CREATE MAPPING t4 TYPE tEsTsTrEaM");
    }

    @Test
    public void when_dropFromPartitionedSchema_then_fail() {
        instance().getMap("my_map").put(42, 43);
        // check that we can query that table
        assertRowsEventuallyInAnyOrder("SELECT * FROM partitioned.my_map", singletonList(new Row(42, 43)));
        assertThatThrownBy(() -> sqlService.execute("DROP MAPPING partitioned.my_map"))
                // TODO a better message would be "You can't delete from 'partitioned' schema", but this is good enough
                .hasMessageContaining("Mapping does not exist: partitioned.my_map");
    }
}
