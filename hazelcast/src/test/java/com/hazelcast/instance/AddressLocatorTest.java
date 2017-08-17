package com.hazelcast.instance;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.AddressLocator;
import org.junit.Test;

import java.net.InetSocketAddress;

public class AddressLocatorTest {
    
    @Test
    public void testLocator() {
        Config config = createConfig();
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        System.out.println("--------------- first instance started. starting another one -----------");
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config);
    }

    private Config createConfig() {
        Config config = new Config();
        config.getNetworkConfig().getAddressLocatorConfig()
                .setEnabled(true)
                .setClassname(MyAddressLocator.class.getName());

        return config;
    }

    public static final class MyAddressLocator implements AddressLocator {

        @Override
        public InetSocketAddress getBindAddress() {
            InetSocketAddress address = new InetSocketAddress("127.0.0.1", 0);
            return address;
        }

        @Override
        public InetSocketAddress getPublicAddress() {
            InetSocketAddress address = new InetSocketAddress("127.0.0.1", 0);
            return address;
        }
    }
}
