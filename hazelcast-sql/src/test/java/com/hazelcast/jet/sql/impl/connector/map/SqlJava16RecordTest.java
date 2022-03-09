package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;
import org.junit.Test;

public class SqlJava16RecordTest extends SqlTestSupport {

    @Test
    public void test() {
        HazelcastInstance inst = createHazelcastInstance(smallInstanceConfig());
        createMapping(inst, "m", Integer.class, Person.class);
        SqlService sqlService = inst.getSql();
        sqlService.execute("insert into m values (1, 42, 'foo')");
        for (SqlRow r : sqlService.execute("select * from m")) {
            System.out.println(r);
        }
    }

    public record Person (String name, int age) {
        Record() { }
    }
}
