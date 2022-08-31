package com.hazelcast.jet.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.impl.JetServiceBackend.SQL_CATALOG_MAP_CONFIG;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JetServiceBackendTest extends JetTestSupport {
    private HazelcastInstance instance;

    @Before
    public void setup() {
        Config config = new Config();
        config.getJetConfig().setEnabled(true);

        instance = createHazelcastInstance(config);
    }

    @Test
    public void when_instanceIsCreated_then_sqlCatalogIsConfigured() {
        MapConfig mapConfig = instance.getConfig().getMapConfig(JetServiceBackend.SQL_CATALOG_MAP_NAME);
        assertEquals(SQL_CATALOG_MAP_CONFIG, mapConfig);
    }
}