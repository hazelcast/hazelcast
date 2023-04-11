/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.dynamicconfig;

import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigCompatibilityChecker;
import com.hazelcast.config.EndpointConfig;
import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.config.IcmpFailureDetectorConfig;
import com.hazelcast.config.InMemoryYamlConfig;
import com.hazelcast.config.MemberAddressProviderConfig;
import com.hazelcast.config.MemcacheProtocolConfig;
import com.hazelcast.config.MulticastConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.RestApiConfig;
import com.hazelcast.config.RestEndpointGroup;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.ServerSocketEndpointConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.config.security.RealmConfig;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.spi.MemberAddressProvider;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Properties;

import static com.hazelcast.config.ConfigCompatibilityChecker.checkEndpointConfigCompatible;
import static com.hazelcast.config.ConfigXmlGenerator.MASK_FOR_SENSITIVE_DATA;
import static com.hazelcast.config.ConfigXmlGeneratorTest.assertFailureDetectorConfigEquals;
import static com.hazelcast.config.ConfigXmlGeneratorTest.multicastConfig;
import static com.hazelcast.config.ConfigXmlGeneratorTest.tcpIpConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DynamicConfigYamlGeneratorTest extends AbstractDynamicConfigGeneratorTest {

    private static final ILogger LOGGER = Logger.getLogger(DynamicConfigYamlGeneratorTest.class);

    // LICENSE KEY

    @Test
    public void testLicenseKey() {
        String licenseKey = randomString();
        Config config = new Config().setLicenseKey(licenseKey);

        Config decConfig = getNewConfigViaGenerator(config);

        String actualLicenseKey = decConfig.getLicenseKey();
        assertEquals(config.getLicenseKey(), actualLicenseKey);
    }

    // Network

    @Test
    public void testIfSensitiveDataIsMasked_whenMaskingEnabled() {
        Config cfg = new Config();
        SSLConfig sslConfig = new SSLConfig();
        sslConfig.setProperty("keyStorePassword", "Hazelcast")
                .setProperty("trustStorePassword", "Hazelcast");
        cfg.getNetworkConfig().setSSLConfig(sslConfig);

        SymmetricEncryptionConfig symmetricEncryptionConfig = new SymmetricEncryptionConfig();
        symmetricEncryptionConfig.setPassword("Hazelcast");
        symmetricEncryptionConfig.setSalt("theSalt");

        cfg.getNetworkConfig().setSymmetricEncryptionConfig(symmetricEncryptionConfig);
        cfg.setLicenseKey("HazelcastLicenseKey");

        Config newConfigViaGenerator = getNewConfigViaGenerator(cfg, true);
        SSLConfig generatedSSLConfig = newConfigViaGenerator.getNetworkConfig().getSSLConfig();

        assertEquals(MASK_FOR_SENSITIVE_DATA, generatedSSLConfig.getProperty("keyStorePassword"));
        assertEquals(MASK_FOR_SENSITIVE_DATA, generatedSSLConfig.getProperty("trustStorePassword"));

        String secPassword = newConfigViaGenerator.getNetworkConfig().getSymmetricEncryptionConfig().getPassword();
        String theSalt = newConfigViaGenerator.getNetworkConfig().getSymmetricEncryptionConfig().getSalt();
        assertEquals(MASK_FOR_SENSITIVE_DATA, secPassword);
        assertEquals(MASK_FOR_SENSITIVE_DATA, theSalt);
        assertEquals(MASK_FOR_SENSITIVE_DATA, newConfigViaGenerator.getLicenseKey());
    }

    @Test
    public void testIfSensitiveDataIsNotMasked_whenMaskingDisabled() {
        String password = "Hazelcast";
        String salt = "theSalt";
        String licenseKey = "HazelcastLicenseKey";

        Config cfg = new Config();
        cfg.getSecurityConfig().setMemberRealmConfig("mr", new RealmConfig().setUsernamePasswordIdentityConfig("user", password));

        SSLConfig sslConfig = new SSLConfig();
        sslConfig.setProperty("keyStorePassword", password)
                .setProperty("trustStorePassword", password);
        cfg.getNetworkConfig().setSSLConfig(sslConfig);

        SymmetricEncryptionConfig symmetricEncryptionConfig = new SymmetricEncryptionConfig();
        symmetricEncryptionConfig.setPassword(password);
        symmetricEncryptionConfig.setSalt(salt);

        cfg.getNetworkConfig().setSymmetricEncryptionConfig(symmetricEncryptionConfig);
        cfg.setLicenseKey(licenseKey);

        Config newConfigViaGenerator = getNewConfigViaGenerator(cfg);
        SSLConfig generatedSSLConfig = newConfigViaGenerator.getNetworkConfig().getSSLConfig();

        assertEquals(password, generatedSSLConfig.getProperty("keyStorePassword"));
        assertEquals(password, generatedSSLConfig.getProperty("trustStorePassword"));

        String secPassword = newConfigViaGenerator.getNetworkConfig().getSymmetricEncryptionConfig().getPassword();
        String theSalt = newConfigViaGenerator.getNetworkConfig().getSymmetricEncryptionConfig().getSalt();
        assertEquals(password, secPassword);
        assertEquals(salt, theSalt);
        assertEquals(licenseKey, newConfigViaGenerator.getLicenseKey());
    }

    private MemberAddressProviderConfig getMemberAddressProviderConfig(Config cfg) {
        MemberAddressProviderConfig expected = cfg.getNetworkConfig().getMemberAddressProviderConfig()
                .setEnabled(true);
        Properties props = expected.getProperties();
        props.setProperty("p1", "v1");
        props.setProperty("p2", "v2");
        props.setProperty("p3", "v3");
        return expected;
    }

    @Test
    public void testMemberAddressProvider() {
        Config cfg = new Config();
        MemberAddressProviderConfig expected = getMemberAddressProviderConfig(cfg)
                .setClassName("ClassName");

        Config newConfigViaGenerator = getNewConfigViaGenerator(cfg);
        MemberAddressProviderConfig actual = newConfigViaGenerator.getNetworkConfig().getMemberAddressProviderConfig();

        assertEquals(expected, actual);
    }

    @Test
    public void testMemberAddressProvider_withImplementation() {
        Config cfg = new Config();
        MemberAddressProviderConfig expected = getMemberAddressProviderConfig(cfg)
                .setImplementation(new TestMemberAddressProvider());

        Config newConfigViaGenerator = getNewConfigViaGenerator(cfg);
        MemberAddressProviderConfig actual = newConfigViaGenerator.getNetworkConfig().getMemberAddressProviderConfig();

        ConfigCompatibilityChecker.checkMemberAddressProviderConfig(expected, actual);
    }

    private static class TestMemberAddressProvider implements MemberAddressProvider {
        @Override
        public InetSocketAddress getBindAddress() {
            return null;
        }

        @Override
        public InetSocketAddress getBindAddress(EndpointQualifier qualifier) {
            return null;
        }

        @Override
        public InetSocketAddress getPublicAddress() {
            return null;
        }

        @Override
        public InetSocketAddress getPublicAddress(EndpointQualifier qualifier) {
            return null;
        }
    }

    @Test
    public void testFailureDetectorConfigGenerator() {
        Config cfg = new Config();
        IcmpFailureDetectorConfig expected = new IcmpFailureDetectorConfig();
        expected.setEnabled(true)
                .setIntervalMilliseconds(1001)
                .setTimeoutMilliseconds(1002)
                .setMaxAttempts(4)
                .setTtl(300)
                .setParallelMode(false) // Defaults to false
                .setFailFastOnStartup(false); // Defaults to false

        cfg.getNetworkConfig().setIcmpFailureDetectorConfig(expected);

        Config newConfigViaGenerator = getNewConfigViaGenerator(cfg);
        IcmpFailureDetectorConfig actual = newConfigViaGenerator.getNetworkConfig().getIcmpFailureDetectorConfig();

        assertFailureDetectorConfigEquals(expected, actual);
    }

    @Test
    public void testNetworkAutoDetectionJoinConfig() {
        Config cfg = new Config();
        cfg.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        Config actualConfig = getNewConfigViaGenerator(cfg);
        assertFalse(actualConfig.getNetworkConfig().getJoin().getAutoDetectionConfig().isEnabled());
    }

    @Test
    public void testNetworkMulticastJoinConfig() {
        Config cfg = new Config();

        MulticastConfig expectedConfig = multicastConfig();

        cfg.getNetworkConfig().getJoin().setMulticastConfig(expectedConfig);

        MulticastConfig actualConfig = getNewConfigViaGenerator(cfg).getNetworkConfig().getJoin().getMulticastConfig();

        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testNetworkTcpJoinConfig() {
        Config cfg = new Config();

        TcpIpConfig expectedConfig = tcpIpConfig();

        cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        cfg.getNetworkConfig().getJoin().setTcpIpConfig(expectedConfig);

        TcpIpConfig actualConfig = getNewConfigViaGenerator(cfg).getNetworkConfig().getJoin().getTcpIpConfig();

        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testNetworkConfigOutboundPorts() {
        Config cfg = new Config();

        NetworkConfig expectedNetworkConfig = cfg.getNetworkConfig();
        expectedNetworkConfig
                .addOutboundPortDefinition("4242-4244")
                .addOutboundPortDefinition("5252;5254");

        NetworkConfig actualNetworkConfig = getNewConfigViaGenerator(cfg).getNetworkConfig();

        assertEquals(expectedNetworkConfig.getOutboundPortDefinitions(), actualNetworkConfig.getOutboundPortDefinitions());
        assertEquals(expectedNetworkConfig.getOutboundPorts(), actualNetworkConfig.getOutboundPorts());
    }

    @Test
    public void testNetworkConfigInterfaces() {
        Config cfg = new Config();

        NetworkConfig expectedNetworkConfig = cfg.getNetworkConfig();
        expectedNetworkConfig.getInterfaces()
                .addInterface("127.0.0.*")
                .setEnabled(true);

        NetworkConfig actualNetworkConfig = getNewConfigViaGenerator(cfg).getNetworkConfig();

        assertEquals(expectedNetworkConfig.getInterfaces(), actualNetworkConfig.getInterfaces());
    }

    private SocketInterceptorConfig createSocketInterceptorConfig() {
        return new SocketInterceptorConfig()
                .setEnabled(true)
                .setProperty("key", "value");
    }

    @Test
    public void testNetworkConfigSocketInterceptor() {
        Config cfg = new Config();

        SocketInterceptorConfig expectedConfig = createSocketInterceptorConfig()
                .setClassName("socketInterceptor");

        cfg.getNetworkConfig().setSocketInterceptorConfig(expectedConfig);

        SocketInterceptorConfig actualConfig = getNewConfigViaGenerator(cfg).getNetworkConfig().getSocketInterceptorConfig();

        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testNetworkConfigSocketInterceptor_interceptorImplementation() {
        Config cfg = new Config();

        SocketInterceptorConfig expectedConfig = createSocketInterceptorConfig()
                .setImplementation(new TestSocketInterceptor());

        cfg.getNetworkConfig().setSocketInterceptorConfig(expectedConfig);

        SocketInterceptorConfig actualConfig = getNewConfigViaGenerator(cfg).getNetworkConfig().getSocketInterceptorConfig();

        ConfigCompatibilityChecker.checkSocketInterceptorConfig(expectedConfig, actualConfig);
    }

    private static class TestSocketInterceptor implements SocketInterceptor {
        @Override
        public void init(Properties properties) {
        }

        @Override
        public void onConnect(Socket connectedSocket) throws IOException {
        }
    }

    @Test
    public void testMemcacheProtocolConfig() {
        MemcacheProtocolConfig memcacheProtocolConfig = new MemcacheProtocolConfig().setEnabled(true);
        Config config = new Config();
        config.getNetworkConfig().setMemcacheProtocolConfig(memcacheProtocolConfig);
        MemcacheProtocolConfig generatedConfig = getNewConfigViaGenerator(config).getNetworkConfig().getMemcacheProtocolConfig();
        assertTrue(generatedConfig.toString() + " should be compatible with " + memcacheProtocolConfig.toString(),
                new ConfigCompatibilityChecker.MemcacheProtocolConfigChecker().check(memcacheProtocolConfig, generatedConfig));
    }

    @Test
    public void testEmptyRestApiConfig() {
        RestApiConfig restApiConfig = new RestApiConfig();
        Config config = new Config();
        config.getNetworkConfig().setRestApiConfig(restApiConfig);
        RestApiConfig generatedConfig = getNewConfigViaGenerator(config).getNetworkConfig().getRestApiConfig();
        assertTrue(generatedConfig.toString() + " should be compatible with " + restApiConfig.toString(),
                new ConfigCompatibilityChecker.RestApiConfigChecker().check(restApiConfig, generatedConfig));
    }

    @Test
    public void testAllEnabledRestApiConfig() {
        RestApiConfig restApiConfig = new RestApiConfig();
        restApiConfig.setEnabled(true).enableAllGroups();
        Config config = new Config();
        config.getNetworkConfig().setRestApiConfig(restApiConfig);
        RestApiConfig generatedConfig = getNewConfigViaGenerator(config).getNetworkConfig().getRestApiConfig();
        assertTrue(generatedConfig.toString() + " should be compatible with " + restApiConfig.toString(),
                new ConfigCompatibilityChecker.RestApiConfigChecker().check(restApiConfig, generatedConfig));
    }

    @Test
    public void testExplicitlyAssignedGroupsRestApiConfig() {
        RestApiConfig restApiConfig = new RestApiConfig();
        restApiConfig.setEnabled(true);
        restApiConfig.enableGroups(RestEndpointGroup.CLUSTER_READ, RestEndpointGroup.HEALTH_CHECK,
                RestEndpointGroup.HOT_RESTART, RestEndpointGroup.WAN);
        restApiConfig.disableGroups(RestEndpointGroup.CLUSTER_WRITE, RestEndpointGroup.DATA);
        Config config = new Config();
        config.getNetworkConfig().setRestApiConfig(restApiConfig);
        RestApiConfig generatedConfig = getNewConfigViaGenerator(config).getNetworkConfig().getRestApiConfig();
        assertTrue(generatedConfig.toString() + " should be compatible with " + restApiConfig.toString(),
                new ConfigCompatibilityChecker.RestApiConfigChecker().check(restApiConfig, generatedConfig));
    }

    @Test
    public void testExplicitlyAssignedGroupsRestApiConfig_whenPersistenceEnabled() {
        RestApiConfig restApiConfig = new RestApiConfig();
        restApiConfig.setEnabled(true);
        restApiConfig.enableGroups(RestEndpointGroup.CLUSTER_READ, RestEndpointGroup.HEALTH_CHECK,
                RestEndpointGroup.PERSISTENCE, RestEndpointGroup.WAN);
        restApiConfig.disableGroups(RestEndpointGroup.CLUSTER_WRITE, RestEndpointGroup.DATA);
        Config config = new Config();
        config.getNetworkConfig().setRestApiConfig(restApiConfig);
        RestApiConfig generatedConfig = getNewConfigViaGenerator(config).getNetworkConfig().getRestApiConfig();
        assertTrue(generatedConfig.toString() + " should be compatible with " + restApiConfig.toString(),
                new ConfigCompatibilityChecker.RestApiConfigChecker().check(restApiConfig, generatedConfig));
    }

    @Test
    public void testExplicitlyAssignedGroupsRestApiConfig_whenBothHotRestartAndPersistenceEnabled() {
        RestApiConfig restApiConfig = new RestApiConfig();
        restApiConfig.setEnabled(true);
        restApiConfig.enableGroups(RestEndpointGroup.CLUSTER_READ, RestEndpointGroup.HEALTH_CHECK,
                RestEndpointGroup.HOT_RESTART, RestEndpointGroup.PERSISTENCE, RestEndpointGroup.WAN);
        restApiConfig.disableGroups(RestEndpointGroup.CLUSTER_WRITE, RestEndpointGroup.DATA);
        Config config = new Config();
        config.getNetworkConfig().setRestApiConfig(restApiConfig);
        RestApiConfig generatedConfig = getNewConfigViaGenerator(config).getNetworkConfig().getRestApiConfig();
        assertTrue(generatedConfig.toString() + " should be compatible with " + restApiConfig.toString(),
                new ConfigCompatibilityChecker.RestApiConfigChecker().check(restApiConfig, generatedConfig));
    }

    @Test
    public void testAdvancedNetworkAutoDetectionJoinConfig() {
        Config cfg = new Config();
        cfg.getAdvancedNetworkConfig().setEnabled(true).getJoin().getAutoDetectionConfig().setEnabled(false);
        Config actualConfig = getNewConfigViaGenerator(cfg);
        assertFalse(actualConfig.getAdvancedNetworkConfig().getJoin().getAutoDetectionConfig().isEnabled());
    }

    @Test
    public void testAdvancedNetworkMulticastJoinConfig() {
        Config cfg = new Config();
        cfg.getAdvancedNetworkConfig().setEnabled(true);
        MulticastConfig expectedConfig = multicastConfig();

        cfg.getAdvancedNetworkConfig().getJoin().setMulticastConfig(expectedConfig);
        cfg.getAdvancedNetworkConfig().setEnabled(true);

        MulticastConfig actualConfig = getNewConfigViaGenerator(cfg)
                .getAdvancedNetworkConfig().getJoin().getMulticastConfig();

        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testAdvancedNetworkTcpJoinConfig() {
        Config cfg = new Config();
        cfg.getAdvancedNetworkConfig().setEnabled(true);
        TcpIpConfig expectedConfig = tcpIpConfig();

        cfg.getAdvancedNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        cfg.getAdvancedNetworkConfig().getJoin().setTcpIpConfig(expectedConfig);

        TcpIpConfig actualConfig = getNewConfigViaGenerator(cfg)
                .getAdvancedNetworkConfig().getJoin().getTcpIpConfig();

        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testAdvancedNetworkFailureDetectorConfigGenerator() {
        Config cfg = new Config();
        IcmpFailureDetectorConfig expected = new IcmpFailureDetectorConfig();
        expected.setEnabled(true)
                .setIntervalMilliseconds(1001)
                .setTimeoutMilliseconds(1002)
                .setMaxAttempts(4)
                .setTtl(300)
                .setParallelMode(false) // Defaults to false
                .setFailFastOnStartup(false); // Defaults to false

        cfg.getAdvancedNetworkConfig().setEnabled(true);
        cfg.getAdvancedNetworkConfig().setIcmpFailureDetectorConfig(expected);

        Config newConfigViaXMLGenerator = getNewConfigViaGenerator(cfg);
        IcmpFailureDetectorConfig actual = newConfigViaXMLGenerator.getAdvancedNetworkConfig().getIcmpFailureDetectorConfig();

        assertFailureDetectorConfigEquals(expected, actual);
    }

    @Test
    public void testAdvancedNetworkMemberAddressProvider() {
        Config cfg = new Config();
        cfg.getAdvancedNetworkConfig().setEnabled(true);
        MemberAddressProviderConfig expected = cfg.getAdvancedNetworkConfig()
                .getMemberAddressProviderConfig();
        expected.setEnabled(true)
                .setEnabled(true)
                .setClassName("ClassName");
        expected.getProperties().setProperty("p1", "v1");

        Config newConfigViaXMLGenerator = getNewConfigViaGenerator(cfg);
        MemberAddressProviderConfig actual = newConfigViaXMLGenerator.getAdvancedNetworkConfig().getMemberAddressProviderConfig();

        assertEquals(expected, actual);
    }

    @Test
    public void testEndpointConfig_completeConfiguration() {
        Config cfg = new Config();

        ServerSocketEndpointConfig expected = new ServerSocketEndpointConfig();
        expected.setName(randomName());
        expected.setPort(9393);
        expected.setPortCount(22);
        expected.setPortAutoIncrement(false);
        expected.setPublicAddress("194.143.14.17");
        expected.setReuseAddress(true);
        expected.addOutboundPortDefinition("4242-4244")
                .addOutboundPortDefinition("5252;5254");

        SocketInterceptorConfig socketInterceptorConfig = new SocketInterceptorConfig()
                .setEnabled(true)
                .setClassName("socketInterceptor")
                .setProperty("key", "value");
        expected.setSocketInterceptorConfig(socketInterceptorConfig);

        expected.getInterfaces().addInterface("127.0.0.*").setEnabled(true);

        expected.setSocketConnectTimeoutSeconds(67);
        expected.setSocketRcvBufferSizeKb(192);
        expected.setSocketSendBufferSizeKb(384);
        expected.setSocketLingerSeconds(3);
        expected.setSocketKeepAlive(true);
        expected.setSocketTcpNoDelay(true);
        expected.setSocketBufferDirect(true);

        cfg.getAdvancedNetworkConfig().setEnabled(true);
        cfg.getAdvancedNetworkConfig().addWanEndpointConfig(expected);

        EndpointConfig actual = getNewConfigViaGenerator(cfg)
                .getAdvancedNetworkConfig().getEndpointConfigs().get(expected.getQualifier());

        checkEndpointConfigCompatible(expected, actual);

    }


    @Test
    public void testDataConnectionConfig() {
        Config expectedConfig = new Config();

        Properties properties = new Properties();
        properties.put("jdbcUrl", "jdbc:h2:mem:" + DynamicConfigYamlGeneratorTest.class.getSimpleName());
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig()
                .setName("test-data-connection")
                .setType("jdbc")
                .setProperties(properties);

        expectedConfig.addDataConnectionConfig(dataConnectionConfig);

        Config actualConfig = getNewConfigViaGenerator(expectedConfig);

        assertEquals(expectedConfig.getDataConnectionConfigs(), actualConfig.getDataConnectionConfigs());
    }

    @Override
    protected Config getNewConfigViaGenerator(Config config) {
        return getNewConfigViaGenerator(config, false);
    }

    protected Config getNewConfigViaGenerator(Config config, boolean maskSensitiveFields) {
        DynamicConfigYamlGenerator dynamicConfigYamlGenerator = new DynamicConfigYamlGenerator();
        String yaml = dynamicConfigYamlGenerator.generate(config, maskSensitiveFields);
        LOGGER.fine("\n" + yaml);
        return new InMemoryYamlConfig(yaml);
    }
}
