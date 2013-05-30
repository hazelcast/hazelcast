package com.hazelcast.client;

import com.hazelcast.test.TestProperties;
import com.hazelcast.test.annotation.ClientCompatibleTest;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.experimental.categories.Categories;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @mdogan 5/28/13
 */

@Ignore
@RunWith(Categories.class)
@Categories.IncludeCategory(ClientCompatibleTest.class)
@Suite.SuiteClasses({})
public class ClientCompatibleTestsSuit {

    @BeforeClass
    public static void setUp() {
        System.setProperty(TestProperties.HAZELCAST_TEST_USE_NETWORK, "true");
        System.setProperty(TestProperties.HAZELCAST_TEST_USE_CLIENT, "true");

        System.setProperty("hazelcast.version.check.enabled", "false");
        System.setProperty("hazelcast.mancenter.enabled", "false");
        System.setProperty("hazelcast.wait.seconds.before.join", "1");
        System.setProperty("hazelcast.local.localAddress", "127.0.0.1");
        System.setProperty("java.net.preferIPv4Stack", "true");
    }
}
