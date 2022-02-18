/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi;

import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.logging.ILogger;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.RootCauseMatcher;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MemberAddressProviderTest {

    @Rule
    public ExpectedException rule = ExpectedException.none();

    @After
    public void tearDown() {
        MemberAddressProviderWithStaticProperties.properties = null;
        MemberAddressProviderWithLogger.logger = null;
        MemberAddressProviderWithPropertiesAndLogger.logger = null;
        MemberAddressProviderWithPropertiesAndLogger.properties = null;
        MemberAddressProviderWithLoggerAndProperties.logger = null;
        MemberAddressProviderWithLoggerAndProperties.properties = null;
        Hazelcast.shutdownAll();
    }

    @Test
    public void testPropertiesAreInjected() {
        final Config config = new Config();
        config.getNetworkConfig().getMemberAddressProviderConfig()
                .setEnabled(true)
                .setClassName(MemberAddressProviderWithStaticProperties.class.getName())
                .getProperties().setProperty("propName", "propValue");

        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);

        Hazelcast.newHazelcastInstance(config);
        final String property = MemberAddressProviderWithStaticProperties.properties.getProperty("propName");
        assertEquals("propValue", property);
    }

    @Test
    public void testLoggerIsInjected() {
        final Config config = new Config();
        config.getNetworkConfig().getMemberAddressProviderConfig()
                .setEnabled(true)
                .setClassName(MemberAddressProviderWithLogger.class.getName());

        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);

        Hazelcast.newHazelcastInstance(config);
        assertNotNull(MemberAddressProviderWithLogger.logger);
    }

    @Test
    public void testLoggerAndPropertiesAreInjected() {
        final Config config = new Config();
        config.getNetworkConfig().getMemberAddressProviderConfig()
                .setEnabled(true)
                .setClassName(MemberAddressProviderWithLoggerAndProperties.class.getName())
                .getProperties().setProperty("propName", "propValue");

        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);

        Hazelcast.newHazelcastInstance(config);
        assertNotNull(MemberAddressProviderWithLoggerAndProperties.logger);
        final String property = MemberAddressProviderWithLoggerAndProperties.properties.getProperty("propName");
        assertEquals("propValue", property);
    }

    @Test
    public void testPropertiesAndLoggerAreInjected() {
        final Config config = new Config();
        config.getNetworkConfig().getMemberAddressProviderConfig()
                .setEnabled(true)
                .setClassName(MemberAddressProviderWithPropertiesAndLogger.class.getName())
                .getProperties().setProperty("propName", "propValue");

        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);

        Hazelcast.newHazelcastInstance(config);
        assertNotNull(MemberAddressProviderWithPropertiesAndLogger.logger);
        final String property = MemberAddressProviderWithPropertiesAndLogger.properties.getProperty("propName");
        assertEquals("propValue", property);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testProviderDoesNotImplementTheInterface() {
        final Config config = new Config();
        config.getNetworkConfig().getMemberAddressProviderConfig()
                .setEnabled(true)
                .setClassName(MemberAddressProviderNotImplementingTheInterface.class.getName());

        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);

        Hazelcast.newHazelcastInstance(config);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testProviderDoesNotHaveMatchingConstructor() {
        final Config config = new Config();
        config.getNetworkConfig().getMemberAddressProviderConfig()
                .setEnabled(true)
                .setClassName(MemberAddressProviderWithoutMatchingConstructor.class.getName());

        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);

        Hazelcast.newHazelcastInstance(config);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testProviderClassDoesNotExist() {
        final Config config = new Config();
        config.getNetworkConfig().getMemberAddressProviderConfig()
                .setEnabled(true)
                .setClassName("com.hazelcast.clazz.does.not.Exist");

        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);

        Hazelcast.newHazelcastInstance(config);
    }

    @Test
    public void testSimpleMemberAddressProviderIsInjected() {
        final Config config = getConfig(SimpleMemberAddressProvider.class);
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);

        final InetSocketAddress inetSocketAddress = (InetSocketAddress) instance.getLocalEndpoint().getSocketAddress();
        assertEquals(inetSocketAddress.getPort(), 9999);

        final Member localMember = instance.getCluster().getLocalMember();
        assertEquals("1.2.3.4", localMember.getAddress().getHost());
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testFailFastWhenNoMatchingConstructorIsFound() {
        final Config config = new Config();
        config.getNetworkConfig().getMemberAddressProviderConfig()
                .setEnabled(true)
                .setClassName(SimpleMemberAddressProvider.class.getName())
                .getProperties().setProperty("foo", "bar"); // <-- this assumes MemberAddressProvider has a constructor accepting Properties

        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);

        //this should fail
        Hazelcast.newHazelcastInstance(config);
    }

    @Test
    public void testImplementationIsUsed() {
        final MemberAddressProvider mock = mock(MemberAddressProvider.class);
        when(mock.getBindAddress()).thenReturn(new InetSocketAddress("localhost", 9999));
        when(mock.getPublicAddress()).thenReturn(new InetSocketAddress("1.2.3.4", 0));

        final Config config = getConfig(mock);
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);

        final InetSocketAddress inetSocketAddress = (InetSocketAddress) instance.getLocalEndpoint().getSocketAddress();
        assertEquals(inetSocketAddress.getPort(), 9999);

        final Member localMember = instance.getCluster().getLocalMember();
        assertEquals("1.2.3.4", localMember.getAddress().getHost());

        verify(mock).getBindAddress();
        verify(mock).getPublicAddress();
    }

    @Test(expected = RuntimeException.class)
    public void instanceFailsToStartWhenGetBindAddressThrowsException() {
        final MemberAddressProvider mock = mock(MemberAddressProvider.class);
        when(mock.getBindAddress()).thenThrow(new RuntimeException("Exception on get bind address"));
        when(mock.getPublicAddress()).thenReturn(new InetSocketAddress("1.2.3.4", 0));

        final Config config = getConfig(mock);

        //this should fail
        Hazelcast.newHazelcastInstance(config);
    }

    @Test(expected = RuntimeException.class)
    public void instanceFailsToStartWhenGetPublicAddressThrowsException() {
        final MemberAddressProvider mock = mock(MemberAddressProvider.class);
        when(mock.getBindAddress()).thenReturn(new InetSocketAddress("localhost", 9999));
        when(mock.getPublicAddress()).thenThrow(new RuntimeException("Exception on get public address"));

        final Config config = getConfig(mock);
        //this should fail
        Hazelcast.newHazelcastInstance(config);
    }

    @Test
    public void instanceFailsToStartWhenAssignedUnbindableAddress() {
        final MemberAddressProvider mock = mock(MemberAddressProvider.class);
        when(mock.getBindAddress()).thenReturn(new InetSocketAddress("1.2.3.4", 9999));
        when(mock.getPublicAddress()).thenReturn(new InetSocketAddress("1.2.3.4", 0));

        final Config config = getConfig(mock);

        // we expect an BindException to be thrown (wrapped in an IllegalStateException)
        rule.expect(HazelcastException.class);
        rule.expect(new RootCauseMatcher(BindException.class));
        Hazelcast.newHazelcastInstance(config);
    }

    private Config getConfig(Class memberAddressProviderClass) {
        Config config = new Config();
        config.getNetworkConfig().getMemberAddressProviderConfig()
                .setEnabled(true)
                .setClassName(memberAddressProviderClass.getName());

        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        return config;
    }

    private Config getConfig(MemberAddressProvider implementation) {
        Config config = new Config();
        config.getNetworkConfig().getMemberAddressProviderConfig()
                .setEnabled(true)
                .setImplementation(implementation);

        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        return config;
    }

    public static final class MemberAddressProviderWithStaticProperties implements MemberAddressProvider {
        public static Properties properties;

        public MemberAddressProviderWithStaticProperties(Properties properties) {
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

        @Override
        public InetSocketAddress getBindAddress(EndpointQualifier qualifier) {
            return new InetSocketAddress("localhost", 0);
        }

        @Override
        public InetSocketAddress getPublicAddress(EndpointQualifier qualifier) {
            return new InetSocketAddress("localhost", 0);
        }
    }

    public static final class MemberAddressProviderWithLogger implements MemberAddressProvider {
        public static ILogger logger;

        public MemberAddressProviderWithLogger(ILogger logger) {
            this.logger = logger;
        }

        @Override
        public InetSocketAddress getBindAddress() {
            return new InetSocketAddress("localhost", 0);
        }

        @Override
        public InetSocketAddress getBindAddress(EndpointQualifier qualifier) {
            return new InetSocketAddress("localhost", 0);
        }

        @Override
        public InetSocketAddress getPublicAddress() {
            return new InetSocketAddress("localhost", 0);
        }

        @Override
        public InetSocketAddress getPublicAddress(EndpointQualifier qualifier) {
            return new InetSocketAddress("localhost", 0);
        }
    }

    public static final class MemberAddressProviderWithLoggerAndProperties implements MemberAddressProvider {
        public static ILogger logger;
        public static Properties properties;


        public MemberAddressProviderWithLoggerAndProperties(ILogger logger, Properties properties) {
            this.logger = logger;
            this.properties = properties;
        }

        @Override
        public InetSocketAddress getBindAddress() {
            return new InetSocketAddress("localhost", 0);
        }

        @Override
        public InetSocketAddress getBindAddress(EndpointQualifier qualifier) {
            return new InetSocketAddress("localhost", 0);
        }

        @Override
        public InetSocketAddress getPublicAddress() {
            return new InetSocketAddress("localhost", 0);
        }

        @Override
        public InetSocketAddress getPublicAddress(EndpointQualifier qualifier) {
            return new InetSocketAddress("localhost", 0);
        }
    }

    public static final class MemberAddressProviderWithPropertiesAndLogger implements MemberAddressProvider {
        public static ILogger logger;
        public static Properties properties;


        public MemberAddressProviderWithPropertiesAndLogger(Properties properties, ILogger logger) {
            this.logger = logger;
            this.properties = properties;
        }

        @Override
        public InetSocketAddress getBindAddress() {
            return new InetSocketAddress("localhost", 0);
        }

        @Override
        public InetSocketAddress getBindAddress(EndpointQualifier qualifier) {
            return new InetSocketAddress("localhost", 0);
        }

        @Override
        public InetSocketAddress getPublicAddress() {
            return new InetSocketAddress("localhost", 0);
        }

        @Override
        public InetSocketAddress getPublicAddress(EndpointQualifier qualifier) {
            return new InetSocketAddress("localhost", 0);
        }
    }

    public static final class SimpleMemberAddressProvider implements MemberAddressProvider {
        @Override
        public InetSocketAddress getBindAddress() {
            return new InetSocketAddress("localhost", 9999);
        }

        @Override
        public InetSocketAddress getPublicAddress() {
            return new InetSocketAddress("1.2.3.4", 0);
        }

        @Override
        public InetSocketAddress getBindAddress(EndpointQualifier qualifier) {
            return new InetSocketAddress("localhost", 9999);
        }

        @Override
        public InetSocketAddress getPublicAddress(EndpointQualifier qualifier) {
            return new InetSocketAddress("1.2.3.4", 0);
        }
    }

    public static final class MemberAddressProviderNotImplementingTheInterface {
    }

    public static final class MemberAddressProviderWithoutMatchingConstructor implements MemberAddressProvider {

        public MemberAddressProviderWithoutMatchingConstructor(String ignored) {

        }

        @Override
        public InetSocketAddress getBindAddress() {
            return new InetSocketAddress("localhost", 9999);
        }

        @Override
        public InetSocketAddress getBindAddress(EndpointQualifier qualifier) {
            return new InetSocketAddress("localhost", 9999);
        }

        @Override
        public InetSocketAddress getPublicAddress() {
            return new InetSocketAddress("1.2.3.4", 0);
        }

        @Override
        public InetSocketAddress getPublicAddress(EndpointQualifier qualifier) {
            return new InetSocketAddress("1.2.3.4", 0);
        }
    }
}
