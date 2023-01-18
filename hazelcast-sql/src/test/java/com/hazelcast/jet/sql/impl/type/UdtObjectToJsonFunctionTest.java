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

package com.hazelcast.jet.sql.impl.type;

import com.hazelcast.config.Config;
import com.hazelcast.jet.sql.SqlJsonTestSupport;
import com.hazelcast.jet.sql.impl.type.BasicNestedFieldsTest.User;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.sql.impl.type.BasicNestedFieldsTest.A;
import static com.hazelcast.jet.sql.impl.type.BasicNestedFieldsTest.B;
import static com.hazelcast.jet.sql.impl.type.BasicNestedFieldsTest.C;
import static com.hazelcast.jet.sql.impl.type.BasicNestedFieldsTest.Office;
import static com.hazelcast.jet.sql.impl.type.BasicNestedFieldsTest.Organization;
import static com.hazelcast.jet.sql.impl.type.BasicNestedFieldsTest.createJavaMapping;
import static com.hazelcast.jet.sql.impl.type.BasicNestedFieldsTest.createJavaType;
import static com.hazelcast.spi.properties.ClusterProperty.SQL_CUSTOM_TYPES_ENABLED;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastSerialClassRunner.class)
public class UdtObjectToJsonFunctionTest extends SqlJsonTestSupport {
    @BeforeClass
    public static void beforeClass() {
        Config config = smallInstanceConfig()
                .setProperty(SQL_CUSTOM_TYPES_ENABLED.getName(), "true");
        initializeWithClient(3, config, null);
    }

    @Test
    public void test_nonCyclic() {
        initDefault();

        assertJsonRowsAnyOrder("SELECT CAST(this AS JSON) FROM test", rows(1, json(
                "{\"organization\":{\"name\":\"organization1\",\"id\":2,\"office\":{\"name\":\"office1\",\"id\":3}},\"id\":1,\"name\":\"user1\"}")));
    }

    @Test
    public void test_failOnCycles() {
        createJavaType(client(), "AType", A.class, "name VARCHAR", "b BType");
        createJavaType(client(), "BType", B.class, "name VARCHAR", "c CType");
        createJavaType(client(), "CType", C.class, "name VARCHAR", "a AType");

        final A a = new A("a");
        final B b = new B("b");
        final C c = new C("c");

        a.b = b;
        b.c = c;
        c.a = a;

        createJavaMapping(client(), "test", A.class, "this AType");
        IMap<Long, A> map = client().getMap("test");
        map.put(1L, a);

        assertThatThrownBy(() -> assertRowsAnyOrder(client(), "SELECT CAST(this AS JSON) FROM test", rows(1, "a")))
                .hasMessageEndingWith("Cycle detected in row value")
                .isInstanceOf(HazelcastSqlException.class)
                .hasFieldOrPropertyWithValue("code", SqlErrorCode.DATA_EXCEPTION);
    }

    @Test
    public void test_returnNullOnNull() {
        initDefault();

        final User user = new User(2L, "user2", null);
        client().getMap("test").put(2L, user);

        assertRowsAnyOrder("SELECT CAST((this).organization AS JSON) FROM test WHERE __key = 2", rows(1, new Object[] {null}));
    }

    @Test
    public void test_castRowAsJsonShouldFail() {
        assertThatThrownBy(() -> assertRowsAnyOrder(client(), "SELECT CAST(v AS JSON) FROM (SELECT (42, 'foo') v)", rows(1, "")))
                .hasMessageEndingWith("CAST function cannot convert value of type ROW to type JSON")
                .isInstanceOf(HazelcastSqlException.class)
                .hasFieldOrPropertyWithValue("code", SqlErrorCode.PARSING);
    }

    private void initDefault() {
        client().getSql().execute(format("CREATE TYPE UserType (id BIGINT, name VARCHAR, organization OrganizationType) "
                + "OPTIONS ('format'='java', 'javaClass'='%s')", User.class.getName()));
        client().getSql().execute(format("CREATE TYPE OrganizationType (id BIGINT, name VARCHAR, office OfficeType) "
                + "OPTIONS ('format'='java', 'javaClass'='%s')", Organization.class.getName()));
        client().getSql().execute(format("CREATE TYPE OfficeType (id BIGINT, name VARCHAR) "
                + "OPTIONS ('format'='java', 'javaClass'='%s')", Office.class.getName()));
        final IMap<Long, User> testMap = client().getMap("test");
        client().getSql().execute("CREATE MAPPING test (__key BIGINT, this UserType) "
                + "TYPE IMap OPTIONS ("
                + "'keyFormat'='bigint', "
                + "'valueFormat'='java', "
                + "'valueJavaClass'='" + User.class.getName() + "')");

        final Office office = new Office(3L, "office1");
        final Organization organization = new Organization(2L, "organization1", office);
        final User user = new User(1L, "user1", organization);
        testMap.put(1L, user);
    }
}
