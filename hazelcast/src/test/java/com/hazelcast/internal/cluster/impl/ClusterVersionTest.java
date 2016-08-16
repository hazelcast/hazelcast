package com.hazelcast.internal.cluster.impl;

import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.Version;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Callable;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClusterVersionTest extends HazelcastTestSupport {

    @Test
    public void test() {
        HazelcastInstance instance = createHazelcastInstance();
        final Cluster cluster = instance.getCluster();

        Version version = cluster.getClusterVersion();

        assertEquals(Version.of("3.8.0"), version);

        cluster.changeClusterVersion(Version.of("3.9.0"));


        assertEqualsEventually(new Callable<Version>() {
            @Override
            public Version call() throws Exception {
                return cluster.getClusterVersion();
            }
        }, Version.of("3.9.0"));
    }

}
