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

package com.hazelcast.jet.sql.impl.type;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.sql.impl.type.BasicNestedFieldsTest.createJavaMapping;
import static com.hazelcast.jet.sql.impl.type.BasicNestedFieldsTest.createJavaType;
import static com.hazelcast.spi.properties.ClusterProperty.SQL_CUSTOM_TYPES_ENABLED;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastSerialClassRunner.class)
public class ToRowJsonFunctionTest extends SqlTestSupport {
    @BeforeClass
    public static void beforeClass() {
        Config config = smallInstanceConfig()
                .setProperty(SQL_CUSTOM_TYPES_ENABLED.getName(), "true");
        initializeWithClient(3, config, null);
    }

    @Test
    public void test_nonCyclic() {
        client().getSql().execute(format("CREATE TYPE UserType (id BIGINT, name VARCHAR, organization OrganizationType) "
                + "OPTIONS ('format'='java', 'javaClass'='%s')", BasicNestedFieldsTest.User.class.getName()));
        client().getSql().execute(format("CREATE TYPE OrganizationType (id BIGINT, name VARCHAR, office OfficeType) "
                + "OPTIONS ('format'='java', 'javaClass'='%s')", BasicNestedFieldsTest.Organization.class.getName()));
        client().getSql().execute(format("CREATE TYPE OfficeType (id BIGINT, name VARCHAR) "
                + "OPTIONS ('format'='java', 'javaClass'='%s')", BasicNestedFieldsTest.Office.class.getName()));
        final IMap<Long, BasicNestedFieldsTest.User> testMap = client().getMap("test");
        client().getSql().execute("CREATE MAPPING test (__key BIGINT, this UserType) "
                + "TYPE IMap OPTIONS ("
                + "'keyFormat'='bigint', "
                + "'valueFormat'='java', "
                + "'valueJavaClass'='" + BasicNestedFieldsTest.User.class.getName() + "')");

        final BasicNestedFieldsTest.Office office = new BasicNestedFieldsTest.Office(3L, "office1");
        final BasicNestedFieldsTest.Organization organization = new BasicNestedFieldsTest.Organization(2L, "organization1", office);
        final BasicNestedFieldsTest.User user = new BasicNestedFieldsTest.User(1L, "user1", organization);
        testMap.put(1L, user);

        assertRowsAnyOrder("SELECT CAST(this AS JSON) FROM test", rows(1,
                new HazelcastJsonValue("[1,\"user1\",[2,\"organization1\",[3,\"office1\"]]]")));
    }

    @Test
    public void test_failOnCycles() {
        createJavaType(client(), "AType", BasicNestedFieldsTest.A.class, "name VARCHAR", "b BType");
        createJavaType(client(), "BType", BasicNestedFieldsTest.B.class, "name VARCHAR", "c CType");
        createJavaType(client(), "CType", BasicNestedFieldsTest.C.class, "name VARCHAR", "a AType");

        final BasicNestedFieldsTest.A a = new BasicNestedFieldsTest.A("a");
        final BasicNestedFieldsTest.B b = new BasicNestedFieldsTest.B("b");
        final BasicNestedFieldsTest.C c = new BasicNestedFieldsTest.C("c");

        a.b = b;
        b.c = c;
        c.a = a;

        createJavaMapping(client(), "test", BasicNestedFieldsTest.A.class, "this AType");
        IMap<Long, BasicNestedFieldsTest.A> map = client().getMap("test");
        map.put(1L, a);

        assertThatThrownBy(() -> assertRowsAnyOrder(client(), "SELECT CAST(this AS JSON) FROM test", rows(1, "a")))
                .hasMessageEndingWith("Cycle detected in row value")
                .isInstanceOf(HazelcastSqlException.class)
                .hasFieldOrPropertyWithValue("code", SqlErrorCode.DATA_EXCEPTION);
    }
}
