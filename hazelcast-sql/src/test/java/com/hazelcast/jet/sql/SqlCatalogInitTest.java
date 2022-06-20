package com.hazelcast.jet.sql;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertFalse;

@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlCatalogInitTest extends SqlTestSupport {
    public static final String MAP_NAME = randomName();
    public static final Config CONFIG = smallInstanceConfig();
    @BeforeClass
    public static void beforeClass() {
        initialize(2, CONFIG);
        createMapping(MAP_NAME, Integer.class, Integer.class);
    }

    @Test
    public void test() {
        HazelcastInstance instance = factory().newHazelcastInstance(CONFIG);
        assertClusterSizeEventually(3, instance);
        waitAllForSafeState(instance);
        try {
            SqlResult result = instance.getSql().execute("select * from " + MAP_NAME);
            assertFalse(result.iterator().hasNext());
        } finally {
            instance.shutdown();
        }
    }
}
