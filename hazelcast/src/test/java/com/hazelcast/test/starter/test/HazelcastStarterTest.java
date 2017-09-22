package com.hazelcast.test.starter.test;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.starter.HazelcastStarter;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class HazelcastStarterTest {

    @Test
    public void testMember() throws InterruptedException {
        HazelcastInstance alwaysRunningMember = HazelcastStarter.newHazelcastInstance("3.7", false);

        for (int i = 1; i < 6; i++) {
            String version = "3.7." + i;
            System.out.println("Starting member " + version);
            HazelcastInstance instance = HazelcastStarter.newHazelcastInstance(version);
            System.out.println("Stopping member " + version);
            instance.shutdown();
        }

        alwaysRunningMember.shutdown();
    }

    @Test
    public void testMemberWithConfig() throws InterruptedException {
        Config config = new Config();
        config.setInstanceName("test-name");

        HazelcastInstance alwaysRunningMember = HazelcastStarter.newHazelcastInstance("3.8", config, false);

        assertEquals(alwaysRunningMember.getName(), "test-name");
        alwaysRunningMember.shutdown();
    }


}
