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
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.type.BasicNestedFieldsTest.A;
import com.hazelcast.jet.sql.impl.type.BasicNestedFieldsTest.B;
import com.hazelcast.jet.sql.impl.type.BasicNestedFieldsTest.C;
import com.hazelcast.jet.sql.impl.type.BasicNestedFieldsTest.SelfRef;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastSerialClassRunner.class)
public class RecurrentStructuresNestedFieldsTest extends SqlTestSupport {

    @BeforeClass
    public static void beforeClass() {
        Config config = smallInstanceConfig()
                .setProperty(ClusterProperty.SQL_CUSTOM_CYCLIC_TYPES_ENABLED.getName(), "true");

        initializeWithClient(2, config, null);
    }

    private static void createJavaMapping(String name, Class<?> valueClass, String... valueFields) {
        BasicNestedFieldsTest.createJavaMapping(client(), name, valueClass, valueFields);
    }

    private static void createType(String name, String... fields) {
        new SqlType(name)
                .fields(fields)
                .create(client());
    }

    @Test
    public void test_selfRefType() {
        createType("SelfRefType", "id BIGINT", "name VARCHAR", "other SelfRefType");

        final SelfRef first = new SelfRef(1L, "first");
        final SelfRef second = new SelfRef(2L, "second");
        final SelfRef third = new SelfRef(3L, "third");
        final SelfRef fourth = new SelfRef(4L, "fourth");

        first.other = second;
        second.other = third;
        third.other = fourth;
        fourth.other = first;

        createJavaMapping("test", SelfRef.class, "this SelfRefType");
        client().getMap("test").put(1L, first);

        // Note: this test was moved from BasicNestedFieldsTest.
        //  The check will cover both client & member behavior.
        List<Row> expectedRows = rows(5,
                "first",
                "second",
                "third",
                "fourth",
                "first"
        );
        assertRowsAnyOrder(client(), "SELECT "
                        + "test.this.name, "
                        + "test.this.other.name, "
                        + "test.this.other.other.name, "
                        + "test.this.other.other.other.name, "
                        + "test.this.other.other.other.other.name "
                        + "FROM test",
                expectedRows);

        assertRowsAnyOrder(instance(), "SELECT "
                        + "test.this.name, "
                        + "test.this.other.name, "
                        + "test.this.other.other.name, "
                        + "test.this.other.other.other.name, "
                        + "test.this.other.other.other.other.name "
                        + "FROM test",
                expectedRows);
    }

    @Test
    public void test_circularlyRecurrentTypes() {
        createType("AType", "name VARCHAR", "b BType");
        createType("BType", "name VARCHAR", "c CType");
        createType("CType", "name VARCHAR", "a AType");

        final A a = new A("a");
        final B b = new B("b");
        final C c = new C("c");

        a.b = b;
        b.c = c;
        c.a = a;

        createJavaMapping("test", A.class, "this AType");
        IMap<Long, A> map = client().getMap("test");
        map.put(1L, a);

        assertRowsAnyOrder(client(), "SELECT (this).b.c.a.name FROM test", rows(1, "a"));
    }

    @Test
    public void test_cyclicTypeUpsertsValidationError() {
        createType("FCA", "name VARCHAR", "b FCB", "c FCC");
        createType("FCB", "name VARCHAR", "a FCA", "c FCC");
        createType("FCC", "name VARCHAR", "a FCA", "b FCB");
        createJavaMapping("tableA", FullyConnectedA.class, "this FCA");
        createJavaMapping("tableB", FullyConnectedB.class, "this FCB");
        createJavaMapping("tableC", FullyConnectedC.class, "this FCC");

        createType("DualGraph", "name VARCHAR", "\"left\" DualGraph", "\"right\" DualGraph");
        createJavaMapping("tableD", DualPathGraph.class, "this DualGraph");

        Consumer<String> assertNotSupported = sql -> assertThatThrownBy(() -> client().getSql().execute(sql))
                .hasMessageContaining("Upserts are not supported for cyclic data type columns");

        assertNotSupported.accept("INSERT INTO tableA VALUES (1, ?)");
        assertNotSupported.accept("INSERT INTO tableB VALUES (1, ?)");
        assertNotSupported.accept("INSERT INTO tableC VALUES (1, ?)");
        assertNotSupported.accept("INSERT INTO tableD VALUES (1, ?)");

        assertNotSupported.accept("UPDATE tableA SET this = ? WHERE __key = 1");
        assertNotSupported.accept("UPDATE tableB SET this = ? WHERE __key = 1");
        assertNotSupported.accept("UPDATE tableC SET this = ? WHERE __key = 1");
        assertNotSupported.accept("UPDATE tableD SET this = ? WHERE __key = 1");
    }

    @Test
    public void test_fullyConnectedGraph() {
        createType("FCA", "name VARCHAR", "b FCB", "c FCC");
        createType("FCB", "name VARCHAR", "a FCA", "c FCC");
        createType("FCC", "name VARCHAR", "a FCA", "b FCB");

        final FullyConnectedA a = new FullyConnectedA("A1");
        final FullyConnectedB b = new FullyConnectedB("B1");
        final FullyConnectedC c = new FullyConnectedC("C1");
        a.setB(b);
        a.setC(c);

        b.setA(a);
        b.setC(c);

        c.setA(a);
        c.setB(b);

        createJavaMapping("tableA", FullyConnectedA.class, "this FCA");
        createJavaMapping("tableB", FullyConnectedB.class, "this FCB");
        createJavaMapping("tableC", FullyConnectedC.class, "this FCC");

        client().getMap("tableA").put(1L, a);
        client().getMap("tableB").put(1L, b);
        client().getMap("tableC").put(1L, c);

        assertRowsAnyOrder(client(), "SELECT (this).b.c.a.name, tableA.this.c.a.name FROM tableA",
                rows(2, "A1", "A1"));
        assertRowsAnyOrder(client(), "SELECT (this).a.c.b.name, tableB.this.a.b.name FROM tableB",
                rows(2, "B1", "B1"));
        assertRowsAnyOrder(client(), "SELECT (this).a.b.c.name, tableC.this.b.c.name FROM tableC",
                rows(2, "C1", "C1"));
    }

    @Test
    public void test_sameTypesDifferentInstances() {
        createType("FCA", "name VARCHAR", "b FCB", "c FCC");
        createType("FCB", "name VARCHAR", "a FCA", "c FCC");
        createType("FCC", "name VARCHAR", "a FCA", "b FCB");

        // A1 -> B1 -> C1 -> A2 -> B2 -> C2 -> <A1>
        final FullyConnectedA a1 = new FullyConnectedA("A1");
        final FullyConnectedB b1 = new FullyConnectedB("B1");
        final FullyConnectedC c1 = new FullyConnectedC("C1");
        final FullyConnectedA a2 = new FullyConnectedA("A2");
        final FullyConnectedB b2 = new FullyConnectedB("B2");
        final FullyConnectedC c2 = new FullyConnectedC("C2");

        a1.setB(b1);
        b1.setC(c1);
        c1.setA(a2);

        a2.setB(b2);
        b2.setC(c2);
        c2.setA(a1);

        createJavaMapping("test", FullyConnectedA.class, "this FCA");
        client().getMap("test").put(1L, a1);

        assertRowsAnyOrder(client(),
                "SELECT "
                + "(this).name, "
                + "(this).b.name, "
                + "(this).b.c.name, "
                + "(this).b.c.a.name, "
                + "(this).b.c.a.b.name, "
                + "(this).b.c.a.b.c.name, "
                + "(this).b.c.a.b.c.a.name "
                + "FROM test", rows(7,
                "A1", "B1", "C1", "A2", "B2", "C2", "A1"));
    }

    @Test
    public void test_treeLikeCyclicGraph() {
        /*
                        A1
                     /     \
                    A2     A3
                  /    \
                 A4      A5
                |   \     \
               [A1][A4]   [A3]
         */
        createType("DualGraph", "name VARCHAR", "\"left\" DualGraph", "\"right\" DualGraph");
        DualPathGraph a1 = new DualPathGraph("A1");
        DualPathGraph a2 = new DualPathGraph("A2");
        DualPathGraph a3 = new DualPathGraph("A3");
        DualPathGraph a4 = new DualPathGraph("A4");
        DualPathGraph a5 = new DualPathGraph("A5");

        a1.setLeft(a2);
        a1.setRight(a3);
        a2.setLeft(a4);
        a2.setRight(a5);

        a4.setLeft(a1);
        a4.setRight(a4);

        a5.setRight(a3);

        createJavaMapping("test", DualPathGraph.class, "this DualGraph");
        client().getMap("test").put(1L, a1);

        assertRowsAnyOrder(client(), "SELECT "
                        + "(this).name, "
                        + "(this).\"left\".name, "
                        + "(this).\"right\".name, "
                        + "(this).\"left\".\"left\".name, "
                        + "(this).\"left\".\"right\".name, "
                        + "(this).\"left\".\"left\".\"left\".name, "
                        + "(this).\"left\".\"left\".\"right\".name, "
                        + "(this).\"left\".\"left\".\"left\".\"right\".name "
                        + " FROM test",
                rows(8, "A1", "A2", "A3", "A4", "A5", "A1", "A4", "A3"));

    }

    public static class FullyConnectedA implements Serializable {
        private String name;
        private FullyConnectedB b;
        private FullyConnectedC c;

        @SuppressWarnings("unused")
        public FullyConnectedA() { }

        public FullyConnectedA(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public FullyConnectedB getB() {
            return b;
        }

        public void setB(FullyConnectedB b) {
            this.b = b;
        }

        public FullyConnectedC getC() {
            return c;
        }

        public void setC(FullyConnectedC c) {
            this.c = c;
        }
    }

    public static class FullyConnectedB implements Serializable {
        private String name;
        private FullyConnectedA a;
        private FullyConnectedC c;

        @SuppressWarnings("unused")
        public FullyConnectedB() { }

        public FullyConnectedB(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public FullyConnectedA getA() {
            return a;
        }

        public void setA(FullyConnectedA a) {
            this.a = a;
        }

        public FullyConnectedC getC() {
            return c;
        }

        public void setC(FullyConnectedC c) {
            this.c = c;
        }
    }

    public static class FullyConnectedC implements Serializable {
        private String name;
        private FullyConnectedA a;
        private FullyConnectedB b;

        @SuppressWarnings("unused")
        public FullyConnectedC() { }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public FullyConnectedA getA() {
            return a;
        }

        public void setA(FullyConnectedA a) {
            this.a = a;
        }

        public FullyConnectedB getB() {
            return b;
        }

        public void setB(FullyConnectedB b) {
            this.b = b;
        }

        public FullyConnectedC(String name) {
            this.name = name;
        }
    }

    public static class DualPathGraph implements Serializable {
        private String name;
        private DualPathGraph left;
        private DualPathGraph right;

        @SuppressWarnings("unused")
        public DualPathGraph() { }

        public DualPathGraph(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public DualPathGraph getLeft() {
            return left;
        }

        public void setLeft(DualPathGraph left) {
            this.left = left;
        }

        public DualPathGraph getRight() {
            return right;
        }

        public void setRight(DualPathGraph right) {
            this.right = right;
        }
    }
}
