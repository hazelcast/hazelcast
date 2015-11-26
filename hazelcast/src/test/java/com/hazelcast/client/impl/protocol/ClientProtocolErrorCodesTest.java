package com.hazelcast.client.impl.protocol;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientProtocolErrorCodesTest extends HazelcastTestSupport {

    @Test
    public void testConstructor() {
        assertUtilityConstructor(ClientProtocolErrorCodes.class);
    }
}
