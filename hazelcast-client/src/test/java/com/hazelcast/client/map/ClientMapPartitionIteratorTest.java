package com.hazelcast.client.map;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientMapPartitionIteratorTest extends AbstractMapPartitionIteratorTest {

    @Before
    public void setup() {
        Config config = getConfig();
        factory = new TestHazelcastFactory();
        server = factory.newHazelcastInstance(config);
        client = factory.newHazelcastClient();
    }
}
