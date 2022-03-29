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

package com.hazelcast.jet.sql.impl.expression;

import com.hazelcast.config.Config;
import com.hazelcast.jet.sql.SqlJsonTestSupport;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.impl.schema.type.TypeRegistry;
import com.hazelcast.sql.impl.type.HazelcastObjectMarker;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Collections;
import java.util.Objects;

@RunWith(HazelcastSerialClassRunner.class)
public class NestedFieldsTest extends SqlJsonTestSupport {

    @BeforeClass
    public static void beforeClass() {
        Config config = new Config();
        config.getJetConfig().setEnabled(true);
        initialize(1, config);
    }

    @Before
    public void init() {
        TypeRegistry.INSTANCE.clear();
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
        assertRowsAnyOrder(sql, rows(4, "user1", "organization1", "office1", 3L));
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
        assertRowsAnyOrder(sql, rows(2, organization, office));
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

        assertRowsAnyOrder(sql, Collections.singletonList(office),
                rows(2, organization, office));
    }

    @Test
    public void test_fullInsert() {
        initDefault();

        final Office office = new Office(5L, "office2");
        final Organization organization = new Organization(4L, "organization2", office);
        final User user = new User(2L, "user1", organization);

        execute("INSERT INTO test (__key, this) VALUES (?, ?)",
                2L, new User(2L, "user2", user.organization));

        assertRowsAnyOrder("SELECT test.this.organization, test.this.organization.office FROM test WHERE __key = 2",
                rows(2, organization, office));
    }

    // TODO: fix
    @Test
    public void test_update() {
        initDefault();

        final Office office = new Office(3L, "office1");
        final Organization organization = new Organization(2L, "organization1", office);
        final User user = new User(1L, "user1", organization);
        final User newUser = new User(1L, "new-name", organization);

        execute("UPDATE test SET this = ? WHERE __key = 1", newUser);
        assertRowsAnyOrder("SELECT test.this.id, test.this.name, test.this.organization FROM test WHERE __key = 1",
                rows(3, 1L, "new-name", organization));
    }

    @Test
    public void test_selfRefType() {
        TypeRegistry.INSTANCE.registerType("SelfRefType", SelfRef.class);

        final SelfRef first = new SelfRef(1L, "first");
        final SelfRef second = new SelfRef(2L, "second");
        final SelfRef third = new SelfRef(3L, "third");
        final SelfRef fourth = new SelfRef(4L, "fourth");

        first.other = second;
        second.other = third;
        third.other = fourth;
        fourth.other = first;

        createMapping("test", Long.class, SelfRef.class);
        instance().getMap("test").put(1L, first);

        assertRowsAnyOrder("SELECT "
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
        TypeRegistry.INSTANCE.registerType("AType", A.class);
        TypeRegistry.INSTANCE.registerType("BType", B.class);
        TypeRegistry.INSTANCE.registerType("CType", C.class);

        final A a = new A("a");
        final B b = new B("b");
        final C c = new C("c");

        a.b = b;
        b.c = c;
        c.a = a;

        createMapping("test", Long.class, A.class);
        IMap<Long, A> map = instance().getMap("test");
        map.put(1L, a);

        assertRowsAnyOrder("SELECT test.this.b.c.a.name FROM test", rows(1, "a"));
    }

    @Test
    public void test_deepInsert() {
        // TODO: ROW-literal?
    }

    // TODO: fix
    @Test
    public void test_deepUpdate() {
        TypeRegistry.INSTANCE.registerType("AType", A.class);
        TypeRegistry.INSTANCE.registerType("BType", B.class);
        TypeRegistry.INSTANCE.registerType("CType", C.class);

        final A a = new A("a");
        final B b = new B("b");
        final C c = new C("c");

        a.b = b;
        b.c = c;
        c.a = a;

        createMapping("public", Long.class, A.class);

//        instance().getSql().execute("CREATE MAPPING public TYPE IMap OPTIONS " +
//                "('keyFormat'='bigint'," +
//                "'valueFormat'='AType')");

        IMap<Long, A> map = instance().getMap("public");
        map.put(1L, a);

        instance().getSql().execute("UPDATE public SET this = (public.this.b, 'a_2')");
        assertRowsAnyOrder("SELECT public.public.this.name, public.public.this.b.c.a.b.c.a.b.name FROM public", rows(2, "a_2", "b"));
    }

    private User initDefault() {
        // TODO: sql, dependent-types
        TypeRegistry.INSTANCE.registerType("UserType", User.class);
        TypeRegistry.INSTANCE.registerType("OfficeType", Office.class);
        TypeRegistry.INSTANCE.registerType("OrganizationType", Organization.class);

        final IMap<Long, User> testMap = instance().getMap("test");
        execute("CREATE MAPPING test "
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

    private void execute(String sql, Object ...args) {
        instance().getSql().execute(sql, args);
    }

    public static class A implements Serializable, HazelcastObjectMarker {
        public String name;
        public B b;

        public A() { }

        public A(final String name) {
            this.name = name;
        }
    }

    public static class B implements Serializable, HazelcastObjectMarker {
        public String name;
        public C c;

        public B() { }

        public B(final String name) {
            this.name = name;
        }
    }

    public static class C implements Serializable, HazelcastObjectMarker {
        public String name;
        public A a;

        public C() { }

        public C(final String name) {
            this.name = name;
        }
    }

    public static class SelfRef implements Serializable, HazelcastObjectMarker {
        public Long id;
        public String name;
        public SelfRef other;

        public SelfRef() { }

        public SelfRef(final Long id, final String name) {
            this.id = id;
            this.name = name;
        }
    }

    public static class User implements Serializable, HazelcastObjectMarker {
        private Long id;
        private String name;
        private Organization organization;

        public User() { }

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

    public static class Organization implements Serializable, Comparable<Organization>, HazelcastObjectMarker {
        private Long id;
        private String name;
        private Office office;

        public Organization() { }

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

    public static class Office implements Serializable, Comparable<Office>, HazelcastObjectMarker {
        private Long id;
        private String name;

        public Office() { }

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
}
