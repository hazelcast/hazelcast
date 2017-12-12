package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class LocalMapStatsProviderTest extends HazelcastTestSupport {

    //https://github.com/hazelcast/hazelcast/issues/11598
    @Test
    public void testRedundantPartitionMigrationWhenManCenterConfigured() {
        Config config = new Config();
        config.getManagementCenterConfig().setEnabled(true);
        config.getManagementCenterConfig().setUrl(format("http://localhost:%d%s/", 8085, "/mancenter"));

        //don't need start management center, just configure it
        final HazelcastInstance instance = createHazelcastInstance(config);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                //check partition migration triggered or not
                int partitionStateVersion = getNode(instance).getPartitionService().getPartitionStateVersion();
                assertEquals(0, partitionStateVersion);
            }
        }, 5);
    }
}
