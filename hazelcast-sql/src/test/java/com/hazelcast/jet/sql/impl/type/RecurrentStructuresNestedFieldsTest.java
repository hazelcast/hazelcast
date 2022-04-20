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

import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.jet.sql.SqlJsonTestSupport;
import com.hazelcast.jet.sql.impl.schema.TypesStorage;
import com.hazelcast.sql.impl.type.HazelcastObjectMarker;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.Serializable;

@RunWith(HazelcastSerialClassRunner.class)
public class RecurrentStructuresNestedFieldsTest extends SqlJsonTestSupport {

    @BeforeClass
    public static void beforeClass() {
        initializeWithClient(1, null, null);
    }

    @Test
    public void test_fullyConnectedGraph() {
        typesStorage().registerType(FullyConnectedA.class.getSimpleName(), FullyConnectedA.class);
        typesStorage().registerType(FullyConnectedB.class.getSimpleName(), FullyConnectedB.class);
        typesStorage().registerType(FullyConnectedC.class.getSimpleName(), FullyConnectedC.class);

        final FullyConnectedA a = new FullyConnectedA("A1");
        final FullyConnectedB b = new FullyConnectedB("B1");
        final FullyConnectedC c = new FullyConnectedC("C1");
        a.setB(b);
        a.setC(c);

        b.setA(a);
        b.setC(c);

        c.setA(a);
        c.setB(b);

        createMapping("tableA", Long.class, FullyConnectedA.class);
        createMapping("tableB", Long.class, FullyConnectedB.class);
        createMapping("tableC", Long.class, FullyConnectedC.class);

        client().getSql().execute("INSERT INTO tableA VALUES (1, ?)", a);
        client().getSql().execute("INSERT INTO tableB VALUES (1, ?)", b);
        client().getSql().execute("INSERT INTO tableC VALUES (1, ?)", c);

        assertRowsAnyOrder(client(), "SELECT tableA.this.b.c.a.name, tableA.this.c.a.name FROM tableA",
                rows(2, "A1", "A1"));
        assertRowsAnyOrder(client(), "SELECT tableB.this.a.c.b.name, tableB.this.a.b.name FROM tableB",
                rows(2, "B1", "B1"));
        assertRowsAnyOrder(client(), "SELECT tableC.this.a.b.c.name, tableC.this.b.c.name FROM tableC",
                rows(2, "C1", "C1"));
    }

    @Test
    public void test_sameTypesDifferentInstances() {
        typesStorage().registerType(FullyConnectedA.class.getSimpleName(), FullyConnectedA.class);
        typesStorage().registerType(FullyConnectedB.class.getSimpleName(), FullyConnectedB.class);
        typesStorage().registerType(FullyConnectedC.class.getSimpleName(), FullyConnectedC.class);

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

        createMapping("test", Long.class, FullyConnectedA.class);
        client().getSql().execute("INSERT INTO test VALUES (1, ?)", a1);

        // TODO: client
        assertRowsAnyOrder(client(),
                "SELECT "
                + "test.this.name, "
                + "test.this.b.name, "
                + "test.this.b.c.name, "
                + "test.this.b.c.a.name, "
                + "test.this.b.c.a.b.name, "
                + "test.this.b.c.a.b.c.name, "
                + "test.this.b.c.a.b.c.a.name "
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
        typesStorage().registerType(DualPathGraph.class.getSimpleName(), DualPathGraph.class);
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

        createMapping("test", Long.class, DualPathGraph.class);
        client().getSql().execute("INSERT INTO test VALUES (1, ?)", a1);

        assertRowsAnyOrder(client(), "SELECT "
                        + "test.this.name, "
                        + "test.this.\"left\".name, "
                        + "test.this.\"right\".name, "
                        + "test.this.\"left\".\"left\".name, "
                        + "test.this.\"left\".\"right\".name, "
                        + "test.this.\"left\".\"left\".\"left\".name, "
                        + "test.this.\"left\".\"left\".\"right\".name, "
                        + "test.this.\"left\".\"left\".\"left\".\"right\".name "
                        + " FROM test",
                rows(8, "A1", "A2", "A3", "A4", "A5", "A1", "A4", "A3"));

    }

    private TypesStorage typesStorage() {
        return new TypesStorage(((HazelcastInstanceProxy) instance()).getOriginal().node.nodeEngine);
    }

    public static class FullyConnectedA implements HazelcastObjectMarker, Serializable {
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

    public static class FullyConnectedB implements HazelcastObjectMarker, Serializable {
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

    public static class FullyConnectedC implements HazelcastObjectMarker, Serializable {
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

    public static class DualPathGraph implements HazelcastObjectMarker, Serializable {
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
