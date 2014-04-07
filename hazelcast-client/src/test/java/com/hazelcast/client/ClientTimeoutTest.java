package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientTimeoutTest {

    @Test(timeout = 20000, expected = IllegalStateException.class)
    public void testTimeoutToOutsideNetwork() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setName( "dev" ).setPassword( "dev-pass" );
        clientConfig.getNetworkConfig().addAddress( "8.8.8.8:5701" );
        HazelcastInstance client = HazelcastClient.newHazelcastClient( clientConfig );
        IList<Object> list = client.getList( "test" );
    }
}
