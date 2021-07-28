package com.hazelcast.sql.impl.expression.json;

import com.hazelcast.config.Config;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JsonQueryIntegrationTest extends SqlTestSupport {
    @BeforeClass
    public static void beforeClass() {
        final Config config = new Config();
        config.getJetConfig().setEnabled(true);
        initialize(1, config);
    }

    @Test
    public void test() {
        final IMap<Long, String> test = instance().getMap("test");
        test.put(1L, "[1,2,3]");
        for (final SqlRow row : instance().getSql().execute("SELECT JSON_QUERY(this, '$[?(@ > 1)]') FROM test")) {
            System.out.println(row);
        }
    }
}
