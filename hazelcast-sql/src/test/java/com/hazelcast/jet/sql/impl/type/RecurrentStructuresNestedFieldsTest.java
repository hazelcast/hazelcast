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
import com.hazelcast.core.HazelcastException;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.Serializable;

import static com.hazelcast.spi.properties.ClusterProperty.SQL_CUSTOM_TYPES_ENABLED;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastSerialClassRunner.class)
public class RecurrentStructuresNestedFieldsTest extends SqlTestSupport {

    @BeforeClass
    public static void beforeClass() {
        Config config = smallInstanceConfig()
                .setProperty(SQL_CUSTOM_TYPES_ENABLED.getName(), "true");

        initializeWithClient(2, config, null);
    }

    @Test
    public void test_cyclicTypeUpsertsValidationError() {
        createJavaType("FCA", FullyConnectedA.class, "name VARCHAR", "b FCB", "c FCC");
        createJavaType("FCB", FullyConnectedB.class, "name VARCHAR", "a FCA", "c FCC");
        createJavaType("FCC", FullyConnectedC.class, "name VARCHAR", "a FCA", "b FCB");
        createJavaMapping("tableA", FullyConnectedA.class, "this FCA");
        createJavaMapping("tableB", FullyConnectedB.class, "this FCB");
        createJavaMapping("tableC", FullyConnectedC.class, "this FCC");

        createJavaType("DualGraph", DualPathGraph.class,
                "name VARCHAR", "\"left\" DualGraph", "\"right\" DualGraph");
        createJavaMapping("tableD", DualPathGraph.class, "this DualGraph");

        assertThatThrownBy(() -> client().getSql().execute("INSERT INTO tableA VALUES (1, ?)"))
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("Upserts are not supported for cyclic data type columns");

        assertThatThrownBy(() -> client().getSql().execute("INSERT INTO tableB VALUES (1, ?)"))
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("Upserts are not supported for cyclic data type columns");

        assertThatThrownBy(() -> client().getSql().execute("INSERT INTO tableC VALUES (1, ?)"))
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("Upserts are not supported for cyclic data type columns");

        assertThatThrownBy(() -> client().getSql().execute("INSERT INTO tableD VALUES (1, ?)"))
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("Upserts are not supported for cyclic data type columns");

        assertThatThrownBy(() -> client().getSql().execute("UPDATE tableA SET this = ? WHERE __key = 1"))
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("Upserts are not supported for cyclic data type columns");

        assertThatThrownBy(() -> client().getSql().execute("UPDATE tableB SET this = ? WHERE __key = 1"))
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("Upserts are not supported for cyclic data type columns");

        assertThatThrownBy(() -> client().getSql().execute("UPDATE tableC SET this = ? WHERE __key = 1"))
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("Upserts are not supported for cyclic data type columns");

        assertThatThrownBy(() -> client().getSql().execute("UPDATE tableD SET this = ? WHERE __key = 1"))
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("Upserts are not supported for cyclic data type columns");
    }

    @Test
    public void test_fullyConnectedGraph() {
        createJavaType("FCA", FullyConnectedA.class, "name VARCHAR", "b FCB", "c FCC");
        createJavaType("FCB", FullyConnectedB.class, "name VARCHAR", "a FCA", "c FCC");
        createJavaType("FCC", FullyConnectedC.class, "name VARCHAR", "a FCA", "b FCB");

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
        createJavaType("FCA", FullyConnectedA.class, "name VARCHAR", "b FCB", "c FCC");
        createJavaType("FCB", FullyConnectedB.class, "name VARCHAR", "a FCA", "c FCC");
        createJavaType("FCC", FullyConnectedC.class, "name VARCHAR", "a FCA", "b FCB");

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
        createJavaType("DualGraph", DualPathGraph.class,
                "name VARCHAR", "\"left\" DualGraph", "\"right\" DualGraph");
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

    private void createJavaType(String name, Class<?> typeClass, String... columns) {
        BasicNestedFieldsTest.createJavaType(client(), name, typeClass, columns);
    }

    private void createJavaMapping(String name, Class<?> javaClass, String... columns) {
        BasicNestedFieldsTest.createJavaMapping(client(), name, javaClass, columns);
    }

    public static class FullyConnectedA implements Serializable {
        private String name;
        private FullyConnectedB b;
        private FullyConnectedC c;

        public FullyConnectedA() { }

        public FullyConnectedA(final String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        public FullyConnectedB getB() {
            return b;
        }

        public void setB(final FullyConnectedB b) {
            this.b = b;
        }

        public FullyConnectedC getC() {
            return c;
        }

        public void setC(final FullyConnectedC c) {
            this.c = c;
        }
    }

    public static class FullyConnectedB implements Serializable {
        private String name;
        private FullyConnectedA a;
        private FullyConnectedC c;

        public FullyConnectedB() { }

        public FullyConnectedB(final String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        public FullyConnectedA getA() {
            return a;
        }

        public void setA(final FullyConnectedA a) {
            this.a = a;
        }

        public FullyConnectedC getC() {
            return c;
        }

        public void setC(final FullyConnectedC c) {
            this.c = c;
        }
    }

    public static class FullyConnectedC implements Serializable {
        private String name;
        private FullyConnectedA a;
        private FullyConnectedB b;

        public FullyConnectedC() { }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        public FullyConnectedA getA() {
            return a;
        }

        public void setA(final FullyConnectedA a) {
            this.a = a;
        }

        public FullyConnectedB getB() {
            return b;
        }

        public void setB(final FullyConnectedB b) {
            this.b = b;
        }

        public FullyConnectedC(final String name) {
            this.name = name;
        }
    }

    public static class DualPathGraph implements Serializable {
        private String name;
        private DualPathGraph left;
        private DualPathGraph right;

        public DualPathGraph() { }

        public DualPathGraph(final String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        public DualPathGraph getLeft() {
            return left;
        }

        public void setLeft(final DualPathGraph left) {
            this.left = left;
        }

        public DualPathGraph getRight() {
            return right;
        }

        public void setRight(final DualPathGraph right) {
            this.right = right;
        }
    }
}
