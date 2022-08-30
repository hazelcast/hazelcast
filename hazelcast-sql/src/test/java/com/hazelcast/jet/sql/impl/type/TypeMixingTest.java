package com.hazelcast.jet.sql.impl.type;

import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.map.model.Person;
import com.hazelcast.spi.properties.ClusterProperty;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.Serializable;

public class TypeMixingTest extends SqlTestSupport {

    private static final int PORTABLE_FACTORY_ID = 123;
    private static final int PORTABLE_CLASS_ID = 456;

    @BeforeClass
    public static void beforeClass() {
        initialize(2, smallInstanceConfig()
                .setProperty(ClusterProperty.SQL_CUSTOM_TYPES_ENABLED.getName(), "true"));
    }

    @Test
    public void test() {
        execute("create type t1(a int) options('format'='java', 'javaClass'='" + Person.class.getName() + "')");
        execute("create type t2(a int) options('format'='java', 'javaClass'='" + Person.class.getName() + "')");

        continue here
    }

    void execute(String sql) {
        instance().getSql().execute(sql);
    }

    private static final class JavaSerializableClass implements Serializable {
        public int fieldSerializable;
    }

    private enum SerializationType {
        JAVA,
        PORTABLE,
        COMPACT
    }
}
