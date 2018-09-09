package com.hazelcast.query.impl.predicates;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SkipIndexSingleMemberTest extends SkipIndexAbstractIntegrationTest {
    private TestHazelcastInstanceFactory hzFactory;

    @Before
    public void before() {
        hzFactory = createHazelcastInstanceFactory(1);
    }

    @After
    public void after() {
        hzFactory.shutdownAll();
    }

    @Override
    public HazelcastInstance getHazelcastInstance() {
        return hzFactory.newInstances()[0];
    }
}
