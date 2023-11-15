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

public class CyclicUDTAreNotAllowedTest extends SqlTestSupport {
    @BeforeClass
    public static void beforeClass() throws Exception {
        initializeWithClient(1, null, null);
    }

    @Test
    public void test_failOnCycles() {
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
    }

    // TODO [sasha]: collect all duplicated usages and move to SqlTestSupport
    private static void createType(String name, String... fields) {
        new SqlType(name)
                .fields(fields)
                .create(client());
    }
}
