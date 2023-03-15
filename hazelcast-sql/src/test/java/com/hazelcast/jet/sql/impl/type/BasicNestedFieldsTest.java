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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.map.model.AllTypesValue;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Date;
import java.util.Objects;

import static com.hazelcast.spi.properties.ClusterProperty.SQL_CUSTOM_TYPES_ENABLED;
import static com.hazelcast.sql.SqlColumnType.OBJECT;
import static java.lang.String.format;
import static java.time.Instant.ofEpochMilli;
import static java.time.ZoneId.systemDefault;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.runners.Parameterized.Parameter;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
public class BasicNestedFieldsTest extends SqlTestSupport {

    @Parameters(name = "useClient:{0}")
    public static Object[] parameters() {
        return new Object[]{false, true};
    }

    @Parameter
    public boolean useClient;

    @BeforeClass
    public static void beforeClass() {
        Config config = smallInstanceConfig()
                .setProperty(SQL_CUSTOM_TYPES_ENABLED.getName(), "true");
        initializeWithClient(3, config, null);
    }

    private HazelcastInstance testInstance() {
        return useClient ? client() : instance();
    }

    @Test
    public void test_simpleNestedColumnSelect() {
        initDefault();

        final String sql = "SELECT "
                + "test.this.name AS user_name, "
                + "test.this.organization.name AS org_name, "
                + "test.this.organization.office.name AS office_name, "
                + "test.this.organization.office.id AS office_id "
                + "FROM test";
        assertRowsAnyOrder(testInstance(), sql, rows(4, "user1", "organization1", "office1", 3L));
    }

    @Test
    public void test_complexProjections() {
        initDefault();
        final String sql = "SELECT "
                + "ABS((this).id) * 2 AS C1, "
                + "FLOOR(CAST(((this).organization).id AS REAL) * 5.0 / 2.0) AS c2"
                + " FROM test";
        assertRowsAnyOrder(testInstance(), sql, rows(2, 2L, 5.0f));
    }

    @Test
    public void test_wholeObjectSelect() {
        final User user = initDefault();
        final Organization organization = user.getOrganization();
        final Office office = organization.getOffice();

        final String sql = "SELECT "
                + "test.this.organization, "
                + "test.this.organization.office "
                + "FROM test";
        SqlResult res = testInstance().getSql().execute(sql);
        assertEquals(OBJECT, res.getRowMetadata().getColumn(0).getType());
        assertRowsAnyOrder(testInstance(), sql, rows(2, organization, office));
    }

    @Test
    public void test_objectComparison() {
        final User user = initDefault();
        final Organization organization = user.getOrganization();
        final Office office = organization.getOffice();

        final String sql = "SELECT "
                + "test.this.organization, "
                + "test.this.organization.office "
                + "FROM test WHERE test.this.organization.office = ?";

        assertRowsAnyOrder(testInstance(), sql, Collections.singletonList(office),
                rows(2, organization, office));
    }

    @Test
    public void test_fullInsert() {
        initDefault();
        execute("CREATE OR REPLACE MAPPING test (__key BIGINT, this UserType) TYPE IMap OPTIONS ("
                + "'keyFormat'='bigint',"
                + "'valueFormat'='java',"
                + format("'valueJavaClass'='%s'", User.class.getName())
                + ")");

        final Office office = new Office(5L, "office2");
        final Organization organization = new Organization(4L, "organization2", office);
        final User user = new User(2L, "user1", organization);

        execute("INSERT INTO test (__key, this) VALUES (?, ?)",
                2L, new User(2L, "user2", user.organization));

        assertRowsAnyOrder(testInstance(), "SELECT test.this.organization, test.this.organization.office FROM test WHERE __key = 2",
                rows(2, organization, office));
    }

    @Test
    public void test_update() {
        final User oldUser = initDefault();

        final User newUser = new User(1L, "new-name", oldUser.organization);

        execute("UPDATE test SET this = ? WHERE __key = 1", newUser);
        assertRowsAnyOrder(testInstance(), "SELECT test.this.id, test.this.name, test.this.organization FROM test WHERE __key = 1",
                rows(3, 1L, "new-name", oldUser.organization));
    }

    @Test
    public void test_selfRefType() {
        createJavaType("SelfRefType", SelfRef.class, "id BIGINT", "other SelfRefType");

        final SelfRef first = new SelfRef(1L, "first");
        final SelfRef second = new SelfRef(2L, "second");
        final SelfRef third = new SelfRef(3L, "third");
        final SelfRef fourth = new SelfRef(4L, "fourth");

        first.other = second;
        second.other = third;
        third.other = fourth;
        fourth.other = first;

        createJavaMapping("test", SelfRef.class,  "this SelfRefType");
        testInstance().getMap("test").put(1L, first);

        assertRowsAnyOrder(testInstance(), "SELECT "
                        + "test.this.name, "
                        + "test.this.other.name, "
                        + "test.this.other.other.name, "
                        + "test.this.other.other.other.name, "
                        + "test.this.other.other.other.other.name "
                        + "FROM test",
                rows(5,
                        "first",
                        "second",
                        "third",
                        "fourth",
                        "first"
                ));
    }

    @Test
    public void test_circularlyRecurrentTypes() {
        createJavaType("AType", A.class, "name VARCHAR", "b BType");
        createJavaType("BType", B.class, "name VARCHAR", "c CType");
        createJavaType("CType", C.class, "name VARCHAR", "a AType");

        final A a = new A("a");
        final B b = new B("b");
        final C c = new C("c");

        a.b = b;
        b.c = c;
        c.a = a;

        createJavaMapping("test", A.class, "this AType");
        IMap<Long, A> map = testInstance().getMap("test");
        map.put(1L, a);

        assertRowsAnyOrder(testInstance(), "SELECT (this).b.c.a.name FROM test", rows(1, "a"));
    }

    @Test
    public void test_deepInsert() {
        initDefault();
        execute("INSERT INTO test VALUES (2, " +
                "(2, 'user2', (2, 'organization2', (2, 'office2'))))");
        assertRowsAnyOrder(testInstance(), "SELECT "
                        + "test.this.name, "
                        + "test.this.organization.name, "
                        + "test.this.organization.office.name "
                        + "FROM test WHERE __key = 2",
                rows(3, "user2", "organization2", "office2"));
    }

    @Test
    public void test_deepUpdate() {
        initDefault();
        execute("UPDATE test SET this = ("
                + "(this).id, "
                + "(this).name, "
                + "("
                + "(this).organization.id, "
                + "(this).organization.name, "
                + "("
                + "(this).organization.office.id,"
                + "'new-office-name'"
                + ")))"
                + "WHERE __key = 1");

        assertRowsAnyOrder("SELECT (this).organization.office.name FROM test WHERE __key = 1",
                rows(1, "new-office-name"));
    }

    @Test
    public void test_mixedModeQuerying() {
        createType("NestedType", NestedPOJO.class);
        createJavaMapping("test", RegularPOJO.class, "name VARCHAR", "child NestedType");

        testInstance().getMap("test")
                .put(1L, new RegularPOJO("parentPojo", new NestedPOJO(1L, "childPojo")));

        assertRowsAnyOrder(testInstance(), "SELECT name, (child).name FROM test", rows(2,
                "parentPojo",
                "childPojo"
        ));

        assertRowsAnyOrder(testInstance(), "SELECT child FROM test", rows(1, new NestedPOJO(1L, "childPojo")));
    }

    @Test
    public void test_mixedModeAliasQuerying() {
        createType("NestedType", NestedPOJO.class);
        execute(format("CREATE MAPPING test ("
                + "__key BIGINT,"
                + "parentName VARCHAR EXTERNAL NAME \"name\","
                + "childObj NestedType EXTERNAL NAME \"child\""
                + ")"
                + "TYPE IMap "
                + "OPTIONS ('keyFormat'='bigint', 'valueFormat'='java', 'valueJavaClass'='%s')", RegularPOJO.class.getName()));

        testInstance().getMap("test")
                .put(1L, new RegularPOJO("parentPojo", new NestedPOJO(1L, "childPojo")));

        assertRowsAnyOrder(testInstance(), "SELECT parentName, (childObj).name FROM (SELECT * FROM test)", rows(2,
                "parentPojo", "childPojo"
        ));
        assertRowsAnyOrder(testInstance(), "SELECT childObj FROM test", rows(1, new NestedPOJO(1L, "childPojo")));
    }

    @Test
    public void test_mixedModeUpsert() {
        createType("NestedType", NestedPOJO.class);
        createJavaMapping("test", RegularPOJO.class, "name VARCHAR", "child NestedType");

        execute("INSERT INTO test (__key, name, child) "
                + "VALUES (1, 'parent', (1, 'child'))");
        assertRowsAnyOrder(testInstance(), "SELECT name, test.child.name FROM test",
                rows(2, "parent", "child"));

        execute("UPDATE test SET child = (2, 'child2')");
        assertRowsAnyOrder(testInstance(), "SELECT test.child.id, test.child.name FROM test",
                rows(2, 2L, "child2"));
    }

    @Test
    public void test_typeCoercionUpserts() {
        createType("AllTypesValue", AllTypesValue.class);
        createJavaMapping("test", AllTypesParent.class, "name VARCHAR", "child AllTypesValue");

        final String allTypesValueRowLiteral = "("
                + "1,"
                + "1,"
                + "true,"
                + "1,"
                + "'1970-01-01T00:00:00Z',"
                + "'A',"
                + "'1970-01-01T00:00:00Z',"
                + "1.0,"
                + "1.0,"
                + "'1970-01-01T00:00:00Z',"
                + "1,"
                + "'1970-01-01',"
                + "'1970-01-01T00:00:00',"
                + "'00:00:00',"
                + "1,"
                + "null,"
                + "null,"
                + "'1970-01-01T00:00:00Z',"
                + "1,"
                + "'test',"
                + "'1970-01-01T00:00:00Z'"
                + ")";

        execute("INSERT INTO test (__key, name, child) VALUES (1, 'parent', "
                + allTypesValueRowLiteral + ")");

        assertRowsAnyOrder(testInstance(), "SELECT "
                + "test.child.bigDecimal,"
                + "test.child.bigInteger,"
                + "test.child.byte0,"
                + "test.child.boolean0,"
                + "test.child.calendar,"
                + "test.child.character0,"
                + "test.child.\"date\","
                + "test.child.double0,"
                + "test.child.float0,"
                + "test.child.instant,"
                + "test.child.int0,"
                + "test.child.localDate,"
                + "test.child.localDateTime,"
                + "test.child.\"localTime\","
                + "test.child.long0,"
                + "test.child.map,"

                + "test.child.object,"
                + "test.child.offsetDateTime,"
                + "test.child.short0,"
                + "test.child.string,"
                + "test.child.zonedDateTime"

                + " FROM test", rows(21,
                new BigDecimal(1L),
                new BigDecimal("1"),
                (byte) 1,
                true,
                OffsetDateTime.from(ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 0, UTC)),
                "A",
                OffsetDateTime.ofInstant(Date.from(ofEpochMilli(0L)).toInstant(), systemDefault()),
                1.0,
                1.0f,
                OffsetDateTime.ofInstant(Instant.ofEpochMilli(0), ZoneOffset.systemDefault()),
                1,
                LocalDate.of(1970, 1, 1),
                LocalDateTime.of(1970, 1, 1, 0, 0, 0),
                LocalTime.of(0, 0, 0),
                1L,
                null,
                null,
                OffsetDateTime.of(1970, 1, 1, 0, 0, 0, 0, UTC),
                (short) 1,
                "test",
                OffsetDateTime.of(1970, 1, 1, 0, 0, 0, 0, UTC)
        ));
        // TODO params
    }

    @Test
    public void test_compoundAliases() {
        initDefault();
        assertRowsAnyOrder(testInstance(), "SELECT ((org).office).name FROM " +
                        "(SELECT (this).organization as org FROM (SELECT * FROM test))",
                rows(1, "office1"));
        assertRowsAnyOrder(testInstance(), "SELECT (((this).organization).office).name FROM (SELECT * FROM test)",
                rows(1, "office1"));
    }

    @Test
    public void test_newDotOperatorSyntax() {
        initDefault();
        assertRowsAnyOrder(testInstance(), "SELECT (((this).organization).office).name FROM test",
                rows(1, "office1"));
    }

    @Test
    public void test_joins() {
        initDefault();
        execute("CREATE MAPPING test2 (__key BIGINT, this UserType) "
                + "TYPE IMap OPTIONS ("
                + "'keyFormat'='bigint', "
                + "'valueFormat'='java', "
                + "'valueJavaClass'='" + User.class.getName() + "')");

        execute("INSERT INTO test2 VALUES (1, (1, 'user2', (1, 'organization2', (1, 'office2'))))");

        assertRowsAnyOrder(testInstance(), "SELECT (((t1.this).organization).office).name, (((t2.this).organization).office).name "
                        + "FROM test AS t1 JOIN test2 AS t2 ON t1.__key = t2.__key",
                rows(2, "office1", "office2"));

        assertRowsAnyOrder(testInstance(), "SELECT (((this).organization).office).name "
                        + "FROM (SELECT t1.this FROM test AS t1 JOIN test2 AS t2 ON t1.__key = t2.__key)",
                rows(1, "office1"));

        assertRowsAnyOrder(testInstance(), "SELECT (((this).organization).office).name "
                        + "FROM (SELECT t2.this FROM test AS t1 JOIN test2 AS t2 ON t1.__key = t2.__key)",
                rows(1, "office2"));

        assertRowsAnyOrder(testInstance(), "SELECT (((this1).organization).office).name, (((this2).organization).office).name "
                        + "FROM (SELECT t1.this as this1, t2.this AS this2 FROM test AS t1 JOIN test2 AS t2 ON t1.__key = t2.__key)",
                rows(2, "office1", "office2"));
    }

    @Test
    public void test_joinsOnNestedFields() {
        initDefault();
        createJavaMapping("test2", User.class, "this UserType");
        execute("INSERT INTO test2 VALUES (1, (1, 'user2', (1, 'organization2', (1, 'office2'))))");

        assertRowsAnyOrder(testInstance(), "SELECT t1.this.organization.office.name, t2.this.organization.office.name "
                        + "FROM test AS t1 JOIN test2 AS t2 "
                        + "ON ABS(t1.this.id) = t2.this.id AND t1.this.id = t2.this.id",
                rows(2, "office1", "office2"));
    }

    @Test
    public void test_missingType() {
        // we create UserType, that has OrganizationType field, but we don't create OrganizationType
        testInstance().getSql().execute(format("CREATE TYPE UserType (id BIGINT, name VARCHAR, organization OrganizationType) "
                + "OPTIONS ('format'='java', 'javaClass'='%s')", User.class.getName()));

        assertThatThrownBy(() -> execute("CREATE MAPPING test (__key BIGINT, this UserType) "
                + "TYPE IMap OPTIONS ("
                + "'keyFormat'='bigint', "
                + "'valueFormat'='java', "
                + "'valueJavaClass'='" + User.class.getName() + "')"))
                .hasMessage("Encountered type 'OrganizationType', which doesn't exist")
                .isInstanceOf(HazelcastSqlException.class);
    }

    @Test
    public void test_nullValueInRow() {
        instance().getSql().execute("CREATE TYPE Office ("
                + "id BIGINT, "
                + "name VARCHAR "
                + ") OPTIONS ('format'='compact', 'compactTypeName'='OfficeCompactType')");

        instance().getSql().execute("CREATE TYPE Organization ("
                + "id BIGINT, "
                + "name VARCHAR, "
                + "office Office"
                + ") OPTIONS ('format'='compact', 'compactTypeName'='OrganizationCompactType')");

        instance().getSql().execute(
                "CREATE MAPPING test ("
                        + "__key BIGINT,"
                        + "organization Organization"
                        + ")"
                        + "TYPE IMap "
                        + "OPTIONS ("
                        + "'keyFormat'='bigint',"
                        + "'valueFormat'='compact',"
                        + "'valueCompactTypeName'='UserCompactType'"
                        + ")");

        instance().getSql().execute("INSERT INTO test VALUES (1, (2, 'orgName', null))");
        assertRowsAnyOrder("SELECT (organization).office FROM test", rows(1, new Object[]{null}));
    }

    private User initDefault() {
        testInstance().getSql().execute(format("CREATE TYPE UserType (id BIGINT, name VARCHAR, organization OrganizationType) "
                + "OPTIONS ('format'='java', 'javaClass'='%s')", User.class.getName()));
        testInstance().getSql().execute(format("CREATE TYPE OrganizationType (id BIGINT, name VARCHAR, office OfficeType) "
                + "OPTIONS ('format'='java', 'javaClass'='%s')", Organization.class.getName()));
        testInstance().getSql().execute(format("CREATE TYPE OfficeType (id BIGINT, name VARCHAR) "
                + "OPTIONS ('format'='java', 'javaClass'='%s')", Office.class.getName()));
        final IMap<Long, User> testMap = testInstance().getMap("test");
        execute("CREATE MAPPING test (__key BIGINT, this UserType) "
                + "TYPE IMap OPTIONS ("
                + "'keyFormat'='bigint', "
                + "'valueFormat'='java', "
                + "'valueJavaClass'='" + User.class.getName() + "')");

        final Office office = new Office(3L, "office1");
        final Organization organization = new Organization(2L, "organization1", office);
        final User user = new User(1L, "user1", organization);
        testMap.put(1L, user);

        return user;
    }

    private void createJavaType(String name, Class<?> typeClass, String... columns) {
        createJavaType(testInstance(), name, typeClass, columns);
    }

    static void createJavaType(HazelcastInstance inst, String name, Class<?> typeClass, String... columns) {
        final String sql = "CREATE TYPE " + name
                + "(" + String.join(",", columns) + ")"
                + "OPTIONS ("
                + "'format'='java',"
                + format("'javaClass'='%s'", typeClass.getName())
                + ")";

        execute(inst, sql);
    }

    private void createJavaMapping(String name, Class<?> javaClass, String... columns) {
        createJavaMapping(testInstance(), name, javaClass, columns);
    }

    static void createJavaMapping(HazelcastInstance inst, String name, Class<?> javaClass, String... columns) {
        final String sql = "CREATE MAPPING " + name
                + "("
                + "__key BIGINT,"
                + String.join(",", columns) +
                ")"
                + " TYPE IMap "
                + "OPTIONS ("
                + "'keyFormat'='bigint',"
                + "'valueFormat'='java',"
                + format("'valueJavaClass'='%s'", javaClass.getName())
                + ")";

        execute(inst, sql);
    }

    void execute(String sql, Object... args) {
        execute(testInstance(), sql, args);
    }

    static void execute(HazelcastInstance inst, String sql, Object... args) {
        inst.getSql().execute(sql, args);
    }

    public static class A implements Serializable {
        public String name;
        public B b;

        public A() {
        }

        public A(final String name) {
            this.name = name;
        }
    }

    public static class B implements Serializable {
        public String name;
        public C c;

        public B() {
        }

        public B(final String name) {
            this.name = name;
        }
    }

    public static class C implements Serializable {
        public String name;
        public A a;

        public C() {
        }

        public C(final String name) {
            this.name = name;
        }
    }

    public static class SelfRef implements Serializable {
        public Long id;
        public String name;
        public SelfRef other;

        public SelfRef() {
        }

        public SelfRef(final Long id, final String name) {
            this.id = id;
            this.name = name;
        }
    }

    public static class User implements Serializable {
        private Long id;
        private String name;
        private Organization organization;

        public User() {
        }

        public User(final Long id, final String name, final Organization organization) {
            this.id = id;
            this.name = name;
            this.organization = organization;
        }

        public Long getId() {
            return id;
        }

        public void setId(final Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        public Organization getOrganization() {
            return organization;
        }

        public void setOrganization(final Organization organization) {
            this.organization = organization;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final User user = (User) o;
            return Objects.equals(id, user.id)
                    && Objects.equals(name, user.name)
                    && Objects.equals(organization, user.organization);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, name, organization);
        }

        @Override
        public String toString() {
            return "User{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    ", organization=" + organization +
                    '}';
        }
    }

    public static class Organization implements Serializable, Comparable<Organization> {
        private Long id;
        private String name;
        private Office office;

        public Organization() {
        }

        public Organization(final Long id, final String name, final Office office) {
            this.id = id;
            this.name = name;
            this.office = office;
        }

        public Long getId() {
            return id;
        }

        public void setId(final Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        public Office getOffice() {
            return office;
        }

        public void setOffice(final Office office) {
            this.office = office;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final Organization that = (Organization) o;
            return Objects.equals(id, that.id)
                    && Objects.equals(name, that.name)
                    && Objects.equals(office, that.office);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, name, office);
        }

        @Override
        public int compareTo(final Organization o) {
            return hashCode() - o.hashCode();
        }

        @Override
        public String toString() {
            return "Organization{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    ", office=" + office +
                    '}';
        }
    }

    public static class Office implements Serializable, Comparable<Office> {
        private Long id;
        private String name;

        public Office() {
        }

        public Office(final Long id, final String name) {
            this.id = id;
            this.name = name;
        }

        public Long getId() {
            return id;
        }

        public void setId(final Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final Office office = (Office) o;
            return Objects.equals(id, office.id) && Objects.equals(name, office.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, name);
        }

        @Override
        public int compareTo(final Office o) {
            return hashCode() - o.hashCode();
        }

        @Override
        public String toString() {
            return "Office{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    '}';
        }
    }

    public static class RegularPOJO implements Serializable {
        private String name;
        private NestedPOJO child;

        public RegularPOJO() {
        }

        public RegularPOJO(final String name, final NestedPOJO child) {
            this.name = name;
            this.child = child;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        public NestedPOJO getChild() {
            return child;
        }

        public void setChild(final NestedPOJO child) {
            this.child = child;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final RegularPOJO that = (RegularPOJO) o;
            return Objects.equals(name, that.name) && Objects.equals(child, that.child);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, child);
        }
    }

    public static class NestedPOJO implements Serializable {
        private Long id;
        private String name;

        public NestedPOJO() {
        }

        public NestedPOJO(final Long id, final String name) {
            this.id = id;
            this.name = name;
        }

        public Long getId() {
            return id;
        }

        public void setId(final Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final NestedPOJO that = (NestedPOJO) o;
            return Objects.equals(id, that.id) && Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, name);
        }
    }

    public static class AllTypesParent implements Serializable {
        private String name;
        private AllTypesValue child;

        public AllTypesParent() {
        }

        public AllTypesParent(final String name, final AllTypesValue child) {
            this.name = name;
            this.child = child;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        public AllTypesValue getChild() {
            return child;
        }

        public void setChild(final AllTypesValue child) {
            this.child = child;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final AllTypesParent that = (AllTypesParent) o;
            return Objects.equals(name, that.name) && Objects.equals(child, that.child);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, child);
        }
    }
}
