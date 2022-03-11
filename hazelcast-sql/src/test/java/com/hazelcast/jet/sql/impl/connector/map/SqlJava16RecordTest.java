package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;
import org.junit.Test;

import java.io.Serializable;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.COMPACT_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JAVA_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;

public class SqlJava16RecordTest extends SqlTestSupport {

    @Test
    public void test() {
        Config config = smallInstanceConfig();
        config.getSerializationConfig().getCompactSerializationConfig().setEnabled(true);
        HazelcastInstance inst = createHazelcastInstance(config);
        SqlService sqlService = inst.getSql();

        inst.getMap("m").put(1, new Person("foo", 42));
        sqlService.execute("CREATE OR REPLACE MAPPING m(__key int, age INT, name VARCHAR) TYPE imap\n"
                + "OPTIONS (\n"
                + '\'' + OPTION_KEY_FORMAT + "'='" + JAVA_FORMAT + "'\n"
                + ", '" + OPTION_KEY_CLASS + "'='" + Integer.class.getName() + "'\n"
                + ", '" + OPTION_VALUE_FORMAT + "'='" + COMPACT_FORMAT + "'\n"
                + ", 'valueCompactTypeName'='" + Person.class.getName() + "'\n"
                + ")"
        );
        sqlService.execute("insert into m values (2, 43, 'foo43')");
        for (SqlRow r : sqlService.execute("select * from m")) {
            System.out.println(r);
        }
    }

    public record Person(String name, int age) implements Serializable {}
}
