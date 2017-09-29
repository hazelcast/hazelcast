/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.spi.addresslocator;

import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigurationException;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.spi.AddressLocator;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.InetSocketAddress;
import java.util.Properties;

import static org.junit.Assert.assertEquals;


@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class AddressLocatorTest {

    @After
    public void tearDown() {
        AddressLocatorWithStaticProperties.properties = null;
        Hazelcast.shutdownAll();
    }

    @Test
    public void testPropertiesAreInjected() {
        Config config = new Config();
        config.getNetworkConfig().getAddressLocatorConfig()
                .setEnabled(true)
                .setClassname(AddressLocatorWithStaticProperties.class.getName())
                .getProperties().setProperty("propName", "propValue");

        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);

        Hazelcast.newHazelcastInstance(config);
        String property = AddressLocatorWithStaticProperties.properties.getProperty("propName");
        assertEquals("propValue", property);
    }

    @Test
    public void testSimpleAddressLocatorIsInjected() {
        Config config = new Config();
        config.getNetworkConfig().getAddressLocatorConfig()
                .setEnabled(true)
                .setClassname(SimpleAddressLocator.class.getName());

        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);

        InetSocketAddress inetSocketAddress = (InetSocketAddress) instance.getLocalEndpoint().getSocketAddress();

        assertEquals(inetSocketAddress.getPort(), 9999);
    }

    @Test(expected = ConfigurationException.class)
    public void testFailFastWhenNoMatchingConstructorIsFound() {
        Config config = new Config();
        config.getNetworkConfig().getAddressLocatorConfig()
                .setEnabled(true)
                .setClassname(SimpleAddressLocator.class.getName())
                .getProperties().setProperty("foo", "bar"); // <-- this assumes AddressLocator has a constructor accepting Properties

        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);

        //this should fail
        Hazelcast.newHazelcastInstance(config);
    }

    @Test
    public void testPublicIPisTakenFromAddressLocator() {
        Config config = new Config();
        config.getNetworkConfig().getAddressLocatorConfig()
                .setEnabled(true)
                .setClassname(SimpleAddressLocator.class.getName());

        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);

        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        Member localMember = instance.getCluster().getLocalMember();

        assertEquals("1.2.3.4", localMember.getAddress().getHost());
    }

    public static final class AddressLocatorWithStaticProperties implements AddressLocator {
        public static Properties properties;

        public AddressLocatorWithStaticProperties(Properties properties) {
            this.properties = properties;
        }

        @Override
        public InetSocketAddress getBindAddress() {
            return new InetSocketAddress("localhost", 0);
        }

        @Override
        public InetSocketAddress getPublicAddress() {
            return new InetSocketAddress("localhost", 0);
        }
    }

    public static final class SimpleAddressLocator implements AddressLocator {
        @Override
        public InetSocketAddress getBindAddress() {
            return new InetSocketAddress("localhost", 9999);
        }

        @Override
        public InetSocketAddress getPublicAddress() {
            return new InetSocketAddress("1.2.3.4", 0);
        }
    }
}
