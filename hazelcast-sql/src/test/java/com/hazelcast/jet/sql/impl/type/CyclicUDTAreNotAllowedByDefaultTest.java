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

import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.type.BasicNestedFieldsTest.A;
import com.hazelcast.jet.sql.impl.type.BasicNestedFieldsTest.B;
import com.hazelcast.jet.sql.impl.type.BasicNestedFieldsTest.C;
import com.hazelcast.map.IMap;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.hazelcast.jet.sql.impl.type.BasicNestedFieldsTest.createJavaMapping;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CyclicUDTAreNotAllowedByDefaultTest extends SqlTestSupport {
    @BeforeClass
    public static void beforeClass() throws Exception {
        initializeWithClient(1, null, null);
    }

    @Test
    public void test_defaultBehaviorFailsOnCycles() {
        createType("AType", "name VARCHAR", "b BType");
        createType("BType", "name VARCHAR", "c CType");
        createType("CType", "name VARCHAR", "a AType");

        final A a = new A("a");
        final B b = new B("b");
        final C c = new C("c");

        a.b = b;
        b.c = c;
        c.a = a;

        createJavaMapping(client(), "test", A.class, "this AType");
        IMap<Long, A> map = client().getMap("test");
        map.put(1L, a);

        assertThatThrownBy(() -> instance().getSql().execute("SELECT * FROM test"))
                .hasMessageContaining("Experimental feature of using cyclic custom types isn't enabled.");

        assertThatThrownBy(() -> instance().getSql().execute("SELECT * FROM test AS t"))
                .hasMessageContaining("Experimental feature of using cyclic custom types isn't enabled.");
    }

    @Test
    public void test_defaultJoinBehaviorFailsOnCycles() {
        createType("AType", "name VARCHAR", "b BType");
        createType("BType", "name VARCHAR", "c CType");
        createType("CType", "name VARCHAR", "a AType");


        createType("SimpleType", "name VARCHAR");

        final A a = new A("a");
        final B b = new B("b");
        final C c = new C("c");

        a.b = b;
        b.c = c;
        c.a = a;

        createJavaMapping(client(), "test", A.class, "this AType");
        createJavaMapping(client(), "simple_test", SimpleType.class, "this SimpleType");

        IMap<Long, A> map = client().getMap("test");
        map.put(1L, a);

        assertThatThrownBy(() ->
                instance().getSql().execute("SELECT * FROM simple_test as t1 JOIN test as t2 on t1.this.name = t2.this.name"))
                .hasMessageContaining("Experimental feature of using cyclic custom types isn't enabled.");
    }

    static class SimpleType {
        String name;

        SimpleType(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    // TODO [sasha]: collect all duplicated usages and move to SqlTestSupport
    private static void createType(String name, String... fields) {
        new SqlType(name)
                .fields(fields)
                .create(client());
    }
}
