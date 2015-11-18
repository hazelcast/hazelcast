package com.hazelcast.internal.management;

import com.hazelcast.internal.management.request.ConsoleRequestConstants;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ConsoleRequestConstantsTest {

    @Test
    public void testPrivateConstructor() {
        HazelcastTestSupport.assertUtilityConstructor(ConsoleRequestConstants.class);
    }
}
