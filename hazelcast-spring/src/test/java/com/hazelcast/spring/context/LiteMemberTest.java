package com.hazelcast.spring.context;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spring.CustomSpringJUnit4ClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;

import static org.junit.Assert.assertTrue;

@RunWith(CustomSpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"test-lite-member-application-context.xml"})
@Category(QuickTest.class)
public class LiteMemberTest {

    @BeforeClass
    @AfterClass
    public static void cleanup() {
        Hazelcast.shutdownAll();
    }

    @Autowired
    private HazelcastInstance instance;

    @Test
    public void shouldBeLiteMember() throws InterruptedException {
        assertTrue(instance.getConfig().isLiteMember());
    }


}