/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import com.hazelcast.collection.QueueStore;
import com.hazelcast.collection.QueueStoreFactory;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.DurationConfig;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig;
import com.hazelcast.config.ConfigCompatibilityChecker.CPSubsystemConfigChecker;
import com.hazelcast.config.ConfigCompatibilityChecker.MetricsConfigChecker;
import com.hazelcast.config.ConfigCompatibilityChecker.SplitBrainProtectionConfigChecker;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.config.cp.FencedLockConfig;
import com.hazelcast.config.cp.SemaphoreConfig;
import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.config.security.JaasAuthenticationConfig;
import com.hazelcast.config.security.KerberosAuthenticationConfig;
import com.hazelcast.config.security.KerberosIdentityConfig;
import com.hazelcast.config.security.LdapAuthenticationConfig;
import com.hazelcast.config.security.LdapRoleMappingMode;
import com.hazelcast.config.security.LdapSearchScope;
import com.hazelcast.config.security.RealmConfig;
import com.hazelcast.config.security.TlsAuthenticationConfig;
import com.hazelcast.config.security.TokenEncoding;
import com.hazelcast.config.security.TokenIdentityConfig;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.MapStore;
import com.hazelcast.map.MapStoreFactory;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.ringbuffer.RingbufferStore;
import com.hazelcast.ringbuffer.RingbufferStoreFactory;
import com.hazelcast.spi.MemberAddressProvider;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryStrategyFactory;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.spi.merge.HigherHitsMergePolicy;
import com.hazelcast.spi.merge.LatestUpdateMergePolicy;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionOn;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.TopicOverloadPolicy;
import com.hazelcast.wan.WanPublisherState;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collection;
import java.util.EventListener;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;
import static com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig.ExpiryPolicyType.ACCESSED;
import static com.hazelcast.config.ConfigCompatibilityChecker.checkEndpointConfigCompatible;
import static com.hazelcast.config.ConfigXmlGenerator.MASK_FOR_SENSITIVE_DATA;
import static com.hazelcast.instance.ProtocolType.MEMBER;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ConfigXmlGeneratorTest extends HazelcastTestSupport {

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

        Config newConfigViaXMLGenerator = getNewConfigViaXMLGenerator(cfg);
        SSLConfig generatedSSLConfig = newConfigViaXMLGenerator.getNetworkConfig().getSSLConfig();

        assertEquals(generatedSSLConfig.getProperty("keyStorePassword"), MASK_FOR_SENSITIVE_DATA);
        assertEquals(generatedSSLConfig.getProperty("trustStorePassword"), MASK_FOR_SENSITIVE_DATA);

        String secPassword = newConfigViaXMLGenerator.getNetworkConfig().getSymmetricEncryptionConfig().getPassword();
        String theSalt = newConfigViaXMLGenerator.getNetworkConfig().getSymmetricEncryptionConfig().getSalt();
        assertEquals(secPassword, MASK_FOR_SENSITIVE_DATA);
        assertEquals(theSalt, MASK_FOR_SENSITIVE_DATA);
        assertEquals(newConfigViaXMLGenerator.getLicenseKey(), MASK_FOR_SENSITIVE_DATA);
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

        Config newConfigViaXMLGenerator = getNewConfigViaXMLGenerator(cfg, false);
        SSLConfig generatedSSLConfig = newConfigViaXMLGenerator.getNetworkConfig().getSSLConfig();

        assertEquals(generatedSSLConfig.getProperty("keyStorePassword"), password);
        assertEquals(generatedSSLConfig.getProperty("trustStorePassword"), password);

        String secPassword = newConfigViaXMLGenerator.getNetworkConfig().getSymmetricEncryptionConfig().getPassword();
        String theSalt = newConfigViaXMLGenerator.getNetworkConfig().getSymmetricEncryptionConfig().getSalt();
        assertEquals(secPassword, password);
        assertEquals(theSalt, salt);
        assertEquals(newConfigViaXMLGenerator.getLicenseKey(), licenseKey);
        SecurityConfig securityConfig = newConfigViaXMLGenerator.getSecurityConfig();
        RealmConfig realmConfig = securityConfig.getRealmConfig(securityConfig.getMemberRealm());
        assertEquals(realmConfig.getUsernamePasswordIdentityConfig().getPassword(), password);
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

        Config newConfigViaXMLGenerator = getNewConfigViaXMLGenerator(cfg);
        MemberAddressProviderConfig actual = newConfigViaXMLGenerator.getNetworkConfig().getMemberAddressProviderConfig();

        assertEquals(expected, actual);
    }

    @Test
    public void testMemberAddressProvider_withImplementation() {
        Config cfg = new Config();
        MemberAddressProviderConfig expected = getMemberAddressProviderConfig(cfg)
            .setImplementation(new TestMemberAddressProvider());

        Config newConfigViaXMLGenerator = getNewConfigViaXMLGenerator(cfg);
        MemberAddressProviderConfig actual = newConfigViaXMLGenerator.getNetworkConfig().getMemberAddressProviderConfig();

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

        Config newConfigViaXMLGenerator = getNewConfigViaXMLGenerator(cfg);
        IcmpFailureDetectorConfig actual = newConfigViaXMLGenerator.getNetworkConfig().getIcmpFailureDetectorConfig();

        assertFailureDetectorConfigEquals(expected, actual);
    }

    @Test
    public void testNetworkMulticastJoinConfig() {
        Config cfg = new Config();

        MulticastConfig expectedConfig = multicastConfig();

        cfg.getNetworkConfig().getJoin().setMulticastConfig(expectedConfig);

        MulticastConfig actualConfig = getNewConfigViaXMLGenerator(cfg).getNetworkConfig().getJoin().getMulticastConfig();

        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testNetworkTcpJoinConfig() {
        Config cfg = new Config();

        TcpIpConfig expectedConfig = tcpIpConfig();

        cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        cfg.getNetworkConfig().getJoin().setTcpIpConfig(expectedConfig);

        TcpIpConfig actualConfig = getNewConfigViaXMLGenerator(cfg).getNetworkConfig().getJoin().getTcpIpConfig();

        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testNetworkConfigOutboundPorts() {
        Config cfg = new Config();

        NetworkConfig expectedNetworkConfig = cfg.getNetworkConfig();
        expectedNetworkConfig
                .addOutboundPortDefinition("4242-4244")
                .addOutboundPortDefinition("5252;5254");

        NetworkConfig actualNetworkConfig = getNewConfigViaXMLGenerator(cfg).getNetworkConfig();

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

        NetworkConfig actualNetworkConfig = getNewConfigViaXMLGenerator(cfg).getNetworkConfig();

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

        SocketInterceptorConfig actualConfig = getNewConfigViaXMLGenerator(cfg).getNetworkConfig().getSocketInterceptorConfig();

        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testNetworkConfigSocketInterceptor_interceptorImplementation() {
        Config cfg = new Config();

        SocketInterceptorConfig expectedConfig = createSocketInterceptorConfig()
            .setImplementation(new TestSocketInterceptor());

        cfg.getNetworkConfig().setSocketInterceptorConfig(expectedConfig);

        SocketInterceptorConfig actualConfig = getNewConfigViaXMLGenerator(cfg).getNetworkConfig().getSocketInterceptorConfig();

        ConfigCompatibilityChecker.checkSocketInterceptorConfig(expectedConfig, actualConfig);
    }

    private static class TestSocketInterceptor implements SocketInterceptor {
        @Override
        public void init(Properties properties) { }

        @Override
        public void onConnect(Socket connectedSocket) throws IOException { }
    }

    @Test
    public void testListenerConfig() {
        Config expectedConfig = new Config();

        expectedConfig.setListenerConfigs(singletonList(new ListenerConfig("Listener")));

        Config actualConfig = getNewConfigViaXMLGenerator(expectedConfig);

        assertEquals(expectedConfig.getListenerConfigs(), actualConfig.getListenerConfigs());
    }

    @Test
    public void testListenerConfig_withImplementation() {
        Config expectedConfig = new Config();

        expectedConfig.setListenerConfigs(singletonList(new ListenerConfig(new TestEventListener())));

        Config actualConfig = getNewConfigViaXMLGenerator(expectedConfig);

        ConfigCompatibilityChecker.checkListenerConfigs(expectedConfig.getListenerConfigs(), actualConfig.getListenerConfigs());
    }

    private static class TestEventListener implements EventListener { }

    @Test
    public void testHotRestartPersistenceConfig() {
        Config cfg = new Config();

        HotRestartPersistenceConfig expectedConfig = cfg.getHotRestartPersistenceConfig();
        expectedConfig.setEnabled(true)
                .setClusterDataRecoveryPolicy(HotRestartClusterDataRecoveryPolicy.FULL_RECOVERY_ONLY)
                .setValidationTimeoutSeconds(100)
                .setDataLoadTimeoutSeconds(130)
                .setBaseDir(new File("nonExisting-base").getAbsoluteFile())
                .setBackupDir(new File("nonExisting-backup").getAbsoluteFile())
                .setParallelism(5).setAutoRemoveStaleData(false);

        HotRestartPersistenceConfig actualConfig = getNewConfigViaXMLGenerator(cfg, false).getHotRestartPersistenceConfig();

        assertEquals(expectedConfig, actualConfig);
    }

    private void configureHotRestartPersistence(Config cfg) {
        cfg.getHotRestartPersistenceConfig()
            .setEnabled(true)
            .setBaseDir(new File("nonExisting-base").getAbsoluteFile())
            .setEncryptionAtRestConfig(
                new EncryptionAtRestConfig()
                    .setEnabled(true)
                    .setAlgorithm("AES")
                    .setSalt("salt")
                    .setSecureStoreConfig(
                        new JavaKeyStoreSecureStoreConfig(new File("path").getAbsoluteFile())
                            .setPassword("keyStorePassword")
                            .setType("JCEKS")
                            .setPollingInterval(60)));
    }

    @Test
    public void testHotRestartPersistenceEncryptionAtRestConfig_whenJavaKeyStore_andMaskingDisabled() {
        Config cfg = new Config();

        configureHotRestartPersistence(cfg);

        HotRestartPersistenceConfig expectedConfig = cfg.getHotRestartPersistenceConfig();

        HotRestartPersistenceConfig actualConfig = getNewConfigViaXMLGenerator(cfg, false).getHotRestartPersistenceConfig();

        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testHotRestartPersistenceEncryptionAtRestConfig_whenJavaKeyStore_andMaskingEnabled() {
        Config cfg = new Config();

        configureHotRestartPersistence(cfg);

        HotRestartPersistenceConfig hrConfig = getNewConfigViaXMLGenerator(cfg).getHotRestartPersistenceConfig();

        EncryptionAtRestConfig actualConfig = hrConfig.getEncryptionAtRestConfig();
        assertTrue(actualConfig.getSecureStoreConfig() instanceof JavaKeyStoreSecureStoreConfig);
        JavaKeyStoreSecureStoreConfig keyStoreConfig = (JavaKeyStoreSecureStoreConfig) actualConfig.getSecureStoreConfig();
        assertEquals(MASK_FOR_SENSITIVE_DATA, keyStoreConfig.getPassword());
    }

    @Test
    public void testHotRestartPersistenceEncryptionAtRestConfig_whenVault_andMaskingEnabled() {
        Config cfg = new Config();

        HotRestartPersistenceConfig expectedConfig = cfg.getHotRestartPersistenceConfig();
        expectedConfig.setEnabled(true)
                .setBaseDir(new File("nonExisting-base").getAbsoluteFile());

        EncryptionAtRestConfig encryptionAtRestConfig = new EncryptionAtRestConfig();
        encryptionAtRestConfig.setEnabled(true);
        encryptionAtRestConfig.setAlgorithm("AES");
        encryptionAtRestConfig.setSalt("salt");
        VaultSecureStoreConfig secureStoreConfig = new VaultSecureStoreConfig("http://address:1234",
                "secret/path", "token");
        secureStoreConfig.setPollingInterval(60);
        SSLConfig sslConfig = new SSLConfig();
        sslConfig.setProperty("keyStorePassword", "Hazelcast")
                .setProperty("trustStorePassword", "Hazelcast");
        secureStoreConfig.setSSLConfig(sslConfig);

        encryptionAtRestConfig.setSecureStoreConfig(secureStoreConfig);

        expectedConfig.setEncryptionAtRestConfig(encryptionAtRestConfig);

        HotRestartPersistenceConfig hrConfig = getNewConfigViaXMLGenerator(cfg).getHotRestartPersistenceConfig();

        EncryptionAtRestConfig actualConfig = hrConfig.getEncryptionAtRestConfig();
        assertTrue(actualConfig.getSecureStoreConfig() instanceof VaultSecureStoreConfig);
        VaultSecureStoreConfig vaultConfig = (VaultSecureStoreConfig) actualConfig.getSecureStoreConfig();
        assertEquals(MASK_FOR_SENSITIVE_DATA, vaultConfig.getToken());
        assertEquals(MASK_FOR_SENSITIVE_DATA, vaultConfig.getSSLConfig().getProperty("keyStorePassword"));
        assertEquals(MASK_FOR_SENSITIVE_DATA, vaultConfig.getSSLConfig().getProperty("trustStorePassword"));
    }

    @Test
    public void testHotRestartPersistenceEncryptionAtRestConfig_whenVault_andMaskingDisabled() {
        Config cfg = new Config();

        HotRestartPersistenceConfig expectedConfig = cfg.getHotRestartPersistenceConfig();
        expectedConfig.setEnabled(true)
                .setBaseDir(new File("nonExisting-base").getAbsoluteFile());

        EncryptionAtRestConfig encryptionAtRestConfig = new EncryptionAtRestConfig();
        encryptionAtRestConfig.setEnabled(true);
        encryptionAtRestConfig.setAlgorithm("AES");
        encryptionAtRestConfig.setSalt("salt");
        VaultSecureStoreConfig secureStoreConfig = new VaultSecureStoreConfig("http://address:1234",
                "secret/path", "token");
        secureStoreConfig.setPollingInterval(60);
        SSLConfig sslConfig = new SSLConfig();
        sslConfig.setProperty("keyStorePassword", "Hazelcast")
                .setProperty("trustStorePassword", "Hazelcast");
        secureStoreConfig.setSSLConfig(sslConfig);

        encryptionAtRestConfig.setSecureStoreConfig(secureStoreConfig);

        expectedConfig.setEncryptionAtRestConfig(encryptionAtRestConfig);

        HotRestartPersistenceConfig actualConfig = getNewConfigViaXMLGenerator(cfg, false).getHotRestartPersistenceConfig();

        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testSecurityConfig() {
        Config cfg = new Config();

        Properties dummyprops = new Properties();
        dummyprops.put("a", "b");

        RealmConfig memberRealm = new RealmConfig().setJaasAuthenticationConfig(new JaasAuthenticationConfig().setLoginModuleConfigs(
                Arrays.asList(
                        new LoginModuleConfig()
                                .setClassName("member.f.o.o")
                                .setUsage(LoginModuleConfig.LoginModuleUsage.OPTIONAL),
                        new LoginModuleConfig()
                                .setClassName("member.b.a.r")
                                .setUsage(LoginModuleConfig.LoginModuleUsage.SUFFICIENT),
                        new LoginModuleConfig()
                                .setClassName("member.l.o.l")
                                .setUsage(LoginModuleConfig.LoginModuleUsage.REQUIRED))))
                .setCredentialsFactoryConfig(new CredentialsFactoryConfig().setClassName("foo.bar").setProperties(dummyprops));
        SecurityConfig expectedConfig = new SecurityConfig();
        expectedConfig.setEnabled(true)
                .setOnJoinPermissionOperation(OnJoinPermissionOperationName.NONE)
                .setClientBlockUnmappedActions(false)
                .setClientRealmConfig("cr", new RealmConfig().setJaasAuthenticationConfig(new JaasAuthenticationConfig().setLoginModuleConfigs(
                        Arrays.asList(
                                new LoginModuleConfig()
                                        .setClassName("f.o.o")
                                        .setUsage(LoginModuleConfig.LoginModuleUsage.OPTIONAL),
                                new LoginModuleConfig()
                                        .setClassName("b.a.r")
                                        .setUsage(LoginModuleConfig.LoginModuleUsage.SUFFICIENT),
                                new LoginModuleConfig()
                                        .setClassName("l.o.l")
                                        .setUsage(LoginModuleConfig.LoginModuleUsage.REQUIRED))))
                        .setUsernamePasswordIdentityConfig("username", "password"))
                .setMemberRealmConfig("mr", memberRealm)
                .setClientPermissionConfigs(new HashSet<>(singletonList(
                        new PermissionConfig()
                                .setActions(newHashSet("read", "remove"))
                                .setEndpoints(newHashSet("127.0.0.1", "127.0.0.2"))
                                .setType(PermissionConfig.PermissionType.ATOMIC_LONG)
                                .setName("mycounter")
                                .setPrincipal("devos"))));

        cfg.setSecurityConfig(expectedConfig);

        SecurityConfig actualConfig = getNewConfigViaXMLGenerator(cfg, false).getSecurityConfig();
        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testLdapConfig() {
        Config cfg = new Config();

        RealmConfig realmConfig = new RealmConfig().setLdapAuthenticationConfig(new LdapAuthenticationConfig()
                .setSkipIdentity(TRUE)
                .setSkipEndpoint(FALSE)
                .setSkipRole(TRUE)
                .setParseDn(true)
                .setPasswordAttribute("passwordAttribute")
                .setRoleContext("roleContext")
                .setRoleFilter("roleFilter")
                .setRoleMappingAttribute("roleMappingAttribute")
                .setRoleMappingMode(LdapRoleMappingMode.REVERSE)
                .setRoleNameAttribute("roleNameAttribute")
                .setRoleRecursionMaxDepth(25)
                .setRoleSearchScope(LdapSearchScope.OBJECT)
                .setSocketFactoryClassName("socketFactoryClassName")
                .setSystemUserDn("systemUserDn")
                .setSystemUserPassword("systemUserPassword")
                .setSystemAuthentication("GSSAPI")
                .setSecurityRealm("krb5Initiator")
                .setUrl("url")
                .setUserContext("userContext")
                .setUserFilter("userFilter")
                .setUserNameAttribute("userNameAttribute")
                .setUserSearchScope(LdapSearchScope.ONE_LEVEL)
                .setSkipAuthentication(TRUE)
        );
        SecurityConfig expectedConfig = new SecurityConfig().setClientRealmConfig("ldapRealm", realmConfig);
        cfg.setSecurityConfig(expectedConfig);

        SecurityConfig actualConfig = getNewConfigViaXMLGenerator(cfg).getSecurityConfig();
        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testKerberosConfig() {
        Config cfg = new Config();

        RealmConfig realmConfig = new RealmConfig()
            .setKerberosAuthenticationConfig(new KerberosAuthenticationConfig()
                .setSkipIdentity(TRUE)
                .setSkipEndpoint(FALSE)
                .setSkipRole(TRUE)
                .setRelaxFlagsCheck(TRUE)
                .setUseNameWithoutRealm(TRUE)
                .setSecurityRealm("jaasRealm")
                .setLdapAuthenticationConfig(new LdapAuthenticationConfig()
                        .setUrl("url")))
            .setKerberosIdentityConfig(new KerberosIdentityConfig()
                .setRealm("HAZELCAST.COM")
                .setSecurityRealm("krb5Init")
                .setServiceNamePrefix("hz/")
                .setUseCanonicalHostname(TRUE)
                .setSpn("spn@HAZELCAST.COM"));
        SecurityConfig expectedConfig = new SecurityConfig().setMemberRealmConfig("kerberosRealm", realmConfig);
        cfg.setSecurityConfig(expectedConfig);

        SecurityConfig actualConfig = getNewConfigViaXMLGenerator(cfg).getSecurityConfig();
        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testTlsAuthenticationConfig() {
        Config cfg = new Config();

        RealmConfig realmConfig = new RealmConfig().setTlsAuthenticationConfig(new TlsAuthenticationConfig()
                .setRoleAttribute("roleAttribute"));
        SecurityConfig expectedConfig = new SecurityConfig().setClientRealmConfig("tlsRealm", realmConfig);
        cfg.setSecurityConfig(expectedConfig);

        SecurityConfig actualConfig = getNewConfigViaXMLGenerator(cfg).getSecurityConfig();
        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testTokenAuthenticationConfig() {
        Config cfg = new Config();

        SecurityConfig expectedConfig = new SecurityConfig()
                .setClientRealmConfig("cRealm",
                        new RealmConfig().setTokenIdentityConfig(new TokenIdentityConfig(TokenEncoding.NONE, "ahoj")))
                .setMemberRealmConfig("mRealm",
                        new RealmConfig().setTokenIdentityConfig(new TokenIdentityConfig(TokenEncoding.BASE64, "bmF6ZGFy")));
        cfg.setSecurityConfig(expectedConfig);

        SecurityConfig actualConfig = getNewConfigViaXMLGenerator(cfg, false).getSecurityConfig();
        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testSerializationConfig() {
        Config cfg = new Config();

        GlobalSerializerConfig globalSerializerConfig = new GlobalSerializerConfig()
                .setClassName("GlobalSerializer")
                .setOverrideJavaSerialization(true);

        SerializerConfig serializerConfig = new SerializerConfig()
                .setClassName(SerializerClass.class.getName())
                .setTypeClassName(TypeClass.class.getName());

        JavaSerializationFilterConfig filterConfig = new JavaSerializationFilterConfig();
        filterConfig.getBlacklist().addClasses("example.Class1", "acme.Test").addPackages("org.infinitban")
                .addPrefixes("dangerous.", "bang");
        filterConfig.getWhitelist().addClasses("WhiteOne", "WhiteTwo").addPackages("com.hazelcast", "test.package")
                .addPrefixes("java");

        SerializationConfig expectedConfig = new SerializationConfig()
                .setAllowUnsafe(true)
                .setPortableVersion(2)
                .setByteOrder(ByteOrder.BIG_ENDIAN)
                .setUseNativeByteOrder(true)
                .setCheckClassDefErrors(true)
                .setEnableCompression(true)
                .setEnableSharedObject(true)
                .setGlobalSerializerConfig(globalSerializerConfig)
                .setJavaSerializationFilterConfig(filterConfig)
                .addDataSerializableFactoryClass(10, "SerializableFactory")
                .addPortableFactoryClass(10, "PortableFactory")
                .addSerializerConfig(serializerConfig);

        cfg.setSerializationConfig(expectedConfig);

        SerializationConfig actualConfig = getNewConfigViaXMLGenerator(cfg).getSerializationConfig();

        assertEquals(expectedConfig.isAllowUnsafe(), actualConfig.isAllowUnsafe());
        assertEquals(expectedConfig.getPortableVersion(), actualConfig.getPortableVersion());
        assertEquals(expectedConfig.getByteOrder(), actualConfig.getByteOrder());
        assertEquals(expectedConfig.isUseNativeByteOrder(), actualConfig.isUseNativeByteOrder());
        assertEquals(expectedConfig.isCheckClassDefErrors(), actualConfig.isCheckClassDefErrors());
        assertEquals(expectedConfig.isEnableCompression(), actualConfig.isEnableCompression());
        assertEquals(expectedConfig.isEnableSharedObject(), actualConfig.isEnableSharedObject());
        assertEquals(expectedConfig.getGlobalSerializerConfig(), actualConfig.getGlobalSerializerConfig());
        assertEquals(expectedConfig.getDataSerializableFactoryClasses(), actualConfig.getDataSerializableFactoryClasses());
        assertEquals(expectedConfig.getPortableFactoryClasses(), actualConfig.getPortableFactoryClasses());
        ConfigCompatibilityChecker.checkSerializerConfigs(expectedConfig.getSerializerConfigs(), actualConfig.getSerializerConfigs());
        assertEquals(expectedConfig.getJavaSerializationFilterConfig(), actualConfig.getJavaSerializationFilterConfig());
    }

    @Test
    public void testSerializationConfig_class() {
        Config cfg = new Config();

        GlobalSerializerConfig globalSerializerConfig = new GlobalSerializerConfig()
                .setClassName("GlobalSerializer")
                .setOverrideJavaSerialization(true);

        SerializerConfig serializerConfig = new SerializerConfig()
                .setImplementation(new SerializerClass())
                .setTypeClass(TypeClass.class);

        JavaSerializationFilterConfig filterConfig = new JavaSerializationFilterConfig();
        filterConfig.getBlacklist().addClasses("example.Class1", "acme.Test").addPackages("org.infinitban")
                .addPrefixes("dangerous.", "bang");
        filterConfig.getWhitelist().addClasses("WhiteOne", "WhiteTwo").addPackages("com.hazelcast", "test.package")
                .addPrefixes("java");

        SerializationConfig expectedConfig = new SerializationConfig()
                .setAllowUnsafe(true)
                .setPortableVersion(2)
                .setByteOrder(ByteOrder.BIG_ENDIAN)
                .setUseNativeByteOrder(true)
                .setCheckClassDefErrors(true)
                .setEnableCompression(true)
                .setEnableSharedObject(true)
                .setGlobalSerializerConfig(globalSerializerConfig)
                .setJavaSerializationFilterConfig(filterConfig)
                .addDataSerializableFactoryClass(10, "SerializableFactory")
                .addPortableFactoryClass(10, "PortableFactory")
                .addSerializerConfig(serializerConfig);

        cfg.setSerializationConfig(expectedConfig);

        SerializationConfig actualConfig = getNewConfigViaXMLGenerator(cfg).getSerializationConfig();

        assertEquals(expectedConfig.isAllowUnsafe(), actualConfig.isAllowUnsafe());
        assertEquals(expectedConfig.getPortableVersion(), actualConfig.getPortableVersion());
        assertEquals(expectedConfig.getByteOrder(), actualConfig.getByteOrder());
        assertEquals(expectedConfig.isUseNativeByteOrder(), actualConfig.isUseNativeByteOrder());
        assertEquals(expectedConfig.isCheckClassDefErrors(), actualConfig.isCheckClassDefErrors());
        assertEquals(expectedConfig.isEnableCompression(), actualConfig.isEnableCompression());
        assertEquals(expectedConfig.isEnableSharedObject(), actualConfig.isEnableSharedObject());
        assertEquals(expectedConfig.getGlobalSerializerConfig(), actualConfig.getGlobalSerializerConfig());
        assertEquals(expectedConfig.getDataSerializableFactoryClasses(), actualConfig.getDataSerializableFactoryClasses());
        assertEquals(expectedConfig.getPortableFactoryClasses(), actualConfig.getPortableFactoryClasses());
        ConfigCompatibilityChecker.checkSerializerConfigs(expectedConfig.getSerializerConfigs(), actualConfig.getSerializerConfigs());
        assertEquals(expectedConfig.getJavaSerializationFilterConfig(), actualConfig.getJavaSerializationFilterConfig());
    }

    private static class TypeClass { }

    private static class SerializerClass implements StreamSerializer {
        @Override
        public void write(ObjectDataOutput out, Object object) throws IOException { }

        @Override
        public Object read(ObjectDataInput in) throws IOException {
            return null;
        }

        @Override
        public int getTypeId() {
            return 0;
        }

        @Override
        public void destroy() { }
    }

    @Test
    public void testPartitionGroupConfig() {
        Config cfg = new Config();

        PartitionGroupConfig expectedConfig = new PartitionGroupConfig()
                .setEnabled(true)
                .setGroupType(PartitionGroupConfig.MemberGroupType.PER_MEMBER)
                .setMemberGroupConfigs(singletonList(new MemberGroupConfig().addInterface("hostname")));

        cfg.setPartitionGroupConfig(expectedConfig);

        PartitionGroupConfig actualConfig = getNewConfigViaXMLGenerator(cfg).getPartitionGroupConfig();

        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testManagementCenterConfigGenerator() {
        ManagementCenterConfig managementCenterConfig = new ManagementCenterConfig()
                .setScriptingEnabled(false)
                .setTrustedInterfaces(newHashSet("192.168.1.1"));

        Config config = new Config()
                .setManagementCenterConfig(managementCenterConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        ManagementCenterConfig xmlMCConfig = xmlConfig.getManagementCenterConfig();
        assertEquals(managementCenterConfig.isScriptingEnabled(), xmlMCConfig.isScriptingEnabled());
        assertEquals(managementCenterConfig.getTrustedInterfaces(), xmlMCConfig.getTrustedInterfaces());
    }

    @Test
    public void testReplicatedMapConfigGenerator() {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig()
                .setPolicy("PassThroughMergePolicy")
                .setBatchSize(1234);

        ReplicatedMapConfig replicatedMapConfig = new ReplicatedMapConfig()
                .setName("replicated-map-name")
                .setStatisticsEnabled(false)
                .setSplitBrainProtectionName("splitBrainProtection")
                .setMergePolicyConfig(mergePolicyConfig)
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .addEntryListenerConfig(new EntryListenerConfig("com.hazelcast.entrylistener", false, false));

        replicatedMapConfig.setAsyncFillup(true);

        Config config = new Config()
                .addReplicatedMapConfig(replicatedMapConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        ReplicatedMapConfig xmlReplicatedMapConfig = xmlConfig.getReplicatedMapConfig("replicated-map-name");
        MergePolicyConfig actualMergePolicyConfig = xmlReplicatedMapConfig.getMergePolicyConfig();
        assertEquals("replicated-map-name", xmlReplicatedMapConfig.getName());
        assertFalse(xmlReplicatedMapConfig.isStatisticsEnabled());
        assertEquals("com.hazelcast.entrylistener", xmlReplicatedMapConfig.getListenerConfigs().get(0).getClassName());
        assertEquals("splitBrainProtection", xmlReplicatedMapConfig.getSplitBrainProtectionName());
        assertEquals(InMemoryFormat.NATIVE, xmlReplicatedMapConfig.getInMemoryFormat());
        assertTrue(xmlReplicatedMapConfig.isAsyncFillup());
        assertEquals("PassThroughMergePolicy", actualMergePolicyConfig.getPolicy());
        assertEquals(1234, actualMergePolicyConfig.getBatchSize());
        assertEquals(replicatedMapConfig, xmlReplicatedMapConfig);
    }

    @Test
    public void testFlakeIdGeneratorConfigGenerator() {
        FlakeIdGeneratorConfig figConfig = new FlakeIdGeneratorConfig("flake-id-gen1")
                .setPrefetchCount(3)
                .setPrefetchValidityMillis(10L)
                .setEpochStart(1000000L)
                .setNodeIdOffset(30L)
                .setBitsSequence(2)
                .setBitsNodeId(3)
                .setAllowedFutureMillis(123L)
                .setStatisticsEnabled(false);

        Config config = new Config()
                .addFlakeIdGeneratorConfig(figConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        FlakeIdGeneratorConfig xmlReplicatedConfig = xmlConfig.getFlakeIdGeneratorConfig("flake-id-gen1");
        assertEquals(figConfig, xmlReplicatedConfig);
    }

    @Test
    public void testCacheAttributes() {
        CacheSimpleConfig expectedConfig = new CacheSimpleConfig()
                .setName("testCache")
                .setEvictionConfig(evictionConfig())
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setBackupCount(2)
                .setAsyncBackupCount(3)
                .setCacheLoader("cacheLoader")
                .setCacheWriter("cacheWriter")
                .setExpiryPolicyFactoryConfig(new ExpiryPolicyFactoryConfig("expiryPolicyFactory"))
                .setManagementEnabled(true)
                .setStatisticsEnabled(true)
                .setKeyType("keyType")
                .setValueType("valueType")
                .setReadThrough(true)
                .setHotRestartConfig(hotRestartConfig())
                .setEventJournalConfig(eventJournalConfig())
                .setCacheEntryListeners(singletonList(cacheSimpleEntryListenerConfig()))
                .setWriteThrough(true)
                .setPartitionLostListenerConfigs(singletonList(
                        new CachePartitionLostListenerConfig("partitionLostListener")))
                .setSplitBrainProtectionName("testSplitBrainProtection");

        expectedConfig.getMergePolicyConfig().setPolicy("mergePolicy");
        expectedConfig.setDisablePerEntryInvalidationEvents(true);
        expectedConfig.setWanReplicationRef(wanReplicationRef());

        Config config = new Config()
                .addCacheConfig(expectedConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        CacheSimpleConfig actualConfig = xmlConfig.getCacheConfig("testCache");
        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testCacheFactoryAttributes() {
        TimedExpiryPolicyFactoryConfig timedExpiryPolicyFactoryConfig = new TimedExpiryPolicyFactoryConfig(ACCESSED,
                new DurationConfig(10, SECONDS));

        CacheSimpleConfig expectedConfig = new CacheSimpleConfig()
                .setName("testCache")
                .setCacheLoaderFactory("cacheLoaderFactory")
                .setCacheWriterFactory("cacheWriterFactory")
                .setExpiryPolicyFactory("expiryPolicyFactory")
                .setCacheEntryListeners(singletonList(cacheSimpleEntryListenerConfig()))
                .setExpiryPolicyFactoryConfig(new ExpiryPolicyFactoryConfig(timedExpiryPolicyFactoryConfig))
                .setPartitionLostListenerConfigs(singletonList(
                        new CachePartitionLostListenerConfig("partitionLostListener")));

        expectedConfig.getMergePolicyConfig().setPolicy("mergePolicy");
        expectedConfig.setDisablePerEntryInvalidationEvents(true);

        Config config = new Config()
                .addCacheConfig(expectedConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        CacheSimpleConfig actualConfig = xmlConfig.getCacheConfig("testCache");
        assertEquals(expectedConfig, actualConfig);
    }

    private static CacheSimpleEntryListenerConfig cacheSimpleEntryListenerConfig() {
        CacheSimpleEntryListenerConfig entryListenerConfig = new CacheSimpleEntryListenerConfig();
        entryListenerConfig.setCacheEntryListenerFactory("entryListenerFactory");
        entryListenerConfig.setSynchronous(true);
        entryListenerConfig.setOldValueRequired(true);
        entryListenerConfig.setCacheEntryEventFilterFactory("entryEventFilterFactory");
        return entryListenerConfig;
    }

    @Test
    public void testCacheSplitBrainProtectionRef() {
        CacheSimpleConfig expectedConfig = new CacheSimpleConfig()
                .setName("testCache")
                .setSplitBrainProtectionName("testSplitBrainProtection");

        Config config = new Config()
                .addCacheConfig(expectedConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        CacheSimpleConfig actualConfig = xmlConfig.getCacheConfig("testCache");
        assertEquals("testSplitBrainProtection", actualConfig.getSplitBrainProtectionName());
    }

    @Test
    public void testRingbufferWithStoreClass() {
        RingbufferStoreConfig ringbufferStoreConfig = new RingbufferStoreConfig()
                .setEnabled(true)
                .setClassName("ClassName")
                .setProperty("p1", "v1")
                .setProperty("p2", "v2")
                .setProperty("p3", "v3");

        testRingbuffer(ringbufferStoreConfig);
    }

    @Test
    public void testRingbufferWithStoreImplementation() {
        RingbufferStoreConfig ringbufferStoreConfig = new RingbufferStoreConfig()
            .setEnabled(true)
            .setStoreImplementation(new TestRingbufferStore())
            .setProperty("p1", "v1")
            .setProperty("p2", "v2")
            .setProperty("p3", "v3");

        testRingbuffer(ringbufferStoreConfig);
    }

    private static class TestRingbufferStore implements RingbufferStore {
        @Override
        public void store(long sequence, Object data) { }

        @Override
        public void storeAll(long firstItemSequence, Object[] items) { }

        @Override
        public Object load(long sequence) {
            return null;
        }

        @Override
        public long getLargestSequence() {
            return 0;
        }
    }

    @Test
    public void testRingbufferWithStoreFactory() {
        RingbufferStoreConfig ringbufferStoreConfig = new RingbufferStoreConfig()
                .setEnabled(true)
                .setFactoryClassName("FactoryClassName")
                .setProperty("p1", "v1")
                .setProperty("p2", "v2")
                .setProperty("p3", "v3");

        testRingbuffer(ringbufferStoreConfig);
    }

    @Test
    public void testRingbufferWithStoreFactoryImplementation() {
        RingbufferStoreConfig ringbufferStoreConfig = new RingbufferStoreConfig()
            .setEnabled(true)
            .setFactoryImplementation(new TestRingbufferStoreFactory())
            .setProperty("p1", "v1")
            .setProperty("p2", "v2")
            .setProperty("p3", "v3");

        testRingbuffer(ringbufferStoreConfig);
    }

    private static class TestRingbufferStoreFactory implements RingbufferStoreFactory {
        @Override
        public RingbufferStore newRingbufferStore(String name, Properties properties) {
            return null;
        }
    }

    private void testRingbuffer(RingbufferStoreConfig ringbufferStoreConfig) {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig()
                .setPolicy("PassThroughMergePolicy")
                .setBatchSize(1234);
        RingbufferConfig expectedConfig = new RingbufferConfig("testRbConfig")
                .setBackupCount(1)
                .setAsyncBackupCount(2)
                .setCapacity(3)
                .setTimeToLiveSeconds(4)
                .setInMemoryFormat(InMemoryFormat.BINARY)
                .setRingbufferStoreConfig(ringbufferStoreConfig)
                .setSplitBrainProtectionName("splitBrainProtection")
                .setMergePolicyConfig(mergePolicyConfig);

        Config config = new Config().addRingBufferConfig(expectedConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        RingbufferConfig actualConfig = xmlConfig.getRingbufferConfig(expectedConfig.getName());
        ConfigCompatibilityChecker.checkRingbufferConfig(expectedConfig, actualConfig);
    }

    @Test
    public void testExecutor() {
        ExecutorConfig expectedConfig = new ExecutorConfig()
                .setName("testExecutor")
                .setStatisticsEnabled(true)
                .setPoolSize(10)
                .setQueueCapacity(100)
                .setSplitBrainProtectionName("splitBrainProtection");

        Config config = new Config()
                .addExecutorConfig(expectedConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        ExecutorConfig actualConfig = xmlConfig.getExecutorConfig(expectedConfig.getName());
        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testDurableExecutor() {
        DurableExecutorConfig expectedConfig = new DurableExecutorConfig()
                .setName("testDurableExecutor")
                .setPoolSize(10)
                .setCapacity(100)
                .setDurability(2)
                .setSplitBrainProtectionName("splitBrainProtection");

        Config config = new Config()
                .addDurableExecutorConfig(expectedConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        DurableExecutorConfig actualConfig = xmlConfig.getDurableExecutorConfig(expectedConfig.getName());
        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testPNCounter() {
        PNCounterConfig expectedConfig = new PNCounterConfig()
                .setName("testPNCounter")
                .setReplicaCount(100)
                .setSplitBrainProtectionName("splitBrainProtection");

        Config config = new Config().addPNCounterConfig(expectedConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        PNCounterConfig actualConfig = xmlConfig.getPNCounterConfig(expectedConfig.getName());
        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testMultiMap() {
        MultiMapConfig expectedConfig = new MultiMapConfig()
                .setName("testMultiMap")
                .setBackupCount(2)
                .setAsyncBackupCount(3)
                .setValueCollectionType(MultiMapConfig.ValueCollectionType.LIST)
                .setBinary(true)
                .setStatisticsEnabled(true)
                .setSplitBrainProtectionName("splitBrainProtection")
                .setEntryListenerConfigs(singletonList(new EntryListenerConfig("java.Listener", true, true)));

        Config config = new Config()
                .addMultiMapConfig(expectedConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        MultiMapConfig actualConfig = xmlConfig.getMultiMapConfig(expectedConfig.getName());
        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testList() {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig()
                .setPolicy(HigherHitsMergePolicy.class.getName())
                .setBatchSize(1234);

        ListConfig expectedConfig = new ListConfig("testList")
                .setMaxSize(10)
                .setStatisticsEnabled(true)
                .setBackupCount(2)
                .setAsyncBackupCount(3)
                .setSplitBrainProtectionName("splitBrainProtection")
                .setMergePolicyConfig(mergePolicyConfig)
                .setItemListenerConfigs(singletonList(new ItemListenerConfig("java.Listener", true)));

        Config config = new Config()
                .addListConfig(expectedConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        ListConfig actualConfig = xmlConfig.getListConfig("testList");
        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testSet() {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig()
                .setPolicy(LatestUpdateMergePolicy.class.getName())
                .setBatchSize(1234);

        SetConfig expectedConfig = new SetConfig("testSet")
                .setMaxSize(10)
                .setStatisticsEnabled(true)
                .setBackupCount(2)
                .setAsyncBackupCount(3)
                .setSplitBrainProtectionName("splitBrainProtection")
                .setMergePolicyConfig(mergePolicyConfig)
                .setItemListenerConfigs(singletonList(new ItemListenerConfig("java.Listener", true)));

        Config config = new Config()
                .addSetConfig(expectedConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        SetConfig actualConfig = xmlConfig.getSetConfig("testSet");
        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testQueueWithStoreClass() {
        QueueStoreConfig queueStoreConfig = new QueueStoreConfig()
                .setClassName("className")
                .setEnabled(true)
                .setProperty("key", "value");

        testQueue(queueStoreConfig);
    }

    @Test
    public void testQueueWithStoreImplementation() {
        QueueStoreConfig queueStoreConfig = new QueueStoreConfig()
                .setStoreImplementation(new TestQueueStore())
                .setEnabled(true)
                .setProperty("key", "value");

        testQueue(queueStoreConfig);
    }

    private static class TestQueueStore implements QueueStore {
        @Override
        public void store(Long key, Object value) { }

        @Override
        public void storeAll(Map map) { }

        @Override
        public void delete(Long key) { }

        @Override
        public void deleteAll(Collection keys) { }

        @Override
        public Object load(Long key) {
            return null;
        }

        @Override
        public Map loadAll(Collection keys) {
            return null;
        }

        @Override
        public Set<Long> loadAllKeys() {
            return null;
        }
    }

    @Test
    public void testQueueWithStoreFactory() {
        QueueStoreConfig queueStoreConfig = new QueueStoreConfig()
                .setFactoryClassName("factoryClassName")
                .setEnabled(true)
                .setProperty("key", "value");

        testQueue(queueStoreConfig);
    }

    @Test
    public void testQueueWithStoreFactoryImplementation() {
        QueueStoreConfig queueStoreConfig = new QueueStoreConfig()
                .setFactoryImplementation(new TestQueueStoreFactory())
                .setEnabled(true)
                .setProperty("key", "value");

        testQueue(queueStoreConfig);
    }

    private static class TestQueueStoreFactory implements QueueStoreFactory {
        @Override
        public QueueStore newQueueStore(String name, Properties properties) {
            return null;
        }
    }

    private void testQueue(QueueStoreConfig queueStoreConfig) {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig()
                .setPolicy(DiscardMergePolicy.class.getSimpleName())
                .setBatchSize(1234);

        QueueConfig expectedConfig = new QueueConfig()
                .setName("testQueue")
                .setMaxSize(10)
                .setStatisticsEnabled(true)
                .setBackupCount(2)
                .setAsyncBackupCount(3)
                .setEmptyQueueTtl(1000)
                .setMergePolicyConfig(mergePolicyConfig)
                .setQueueStoreConfig(queueStoreConfig)
                .setItemListenerConfigs(singletonList(new ItemListenerConfig("java.Listener", true)));

        Config config = new Config()
                .addQueueConfig(expectedConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        QueueConfig actualConfig = xmlConfig.getQueueConfig("testQueue");
        assertEquals("testQueue", actualConfig.getName());

        MergePolicyConfig xmlMergePolicyConfig = actualConfig.getMergePolicyConfig();
        assertEquals(DiscardMergePolicy.class.getSimpleName(), xmlMergePolicyConfig.getPolicy());
        assertEquals(1234, xmlMergePolicyConfig.getBatchSize());
        ConfigCompatibilityChecker.checkQueueConfig(expectedConfig, actualConfig);
    }

    @Test
    public void testNativeMemory() {
        NativeMemoryConfig expectedConfig = new NativeMemoryConfig();
        expectedConfig.setEnabled(true);
        expectedConfig.setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.STANDARD);
        expectedConfig.setMetadataSpacePercentage(12.5f);
        expectedConfig.setMinBlockSize(50);
        expectedConfig.setPageSize(100);
        expectedConfig.setSize(new MemorySize(20, MemoryUnit.MEGABYTES));

        Config config = new Config().setNativeMemoryConfig(expectedConfig);
        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        NativeMemoryConfig actualConfig = xmlConfig.getNativeMemoryConfig();
        assertTrue(actualConfig.isEnabled());
        assertEquals(NativeMemoryConfig.MemoryAllocatorType.STANDARD, actualConfig.getAllocatorType());
        assertEquals(12.5, actualConfig.getMetadataSpacePercentage(), 0.0001);
        assertEquals(50, actualConfig.getMinBlockSize());
        assertEquals(100, actualConfig.getPageSize());
        assertEquals(new MemorySize(20, MemoryUnit.MEGABYTES).getUnit(), actualConfig.getSize().getUnit());
        assertEquals(new MemorySize(20, MemoryUnit.MEGABYTES).getValue(), actualConfig.getSize().getValue());
        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testAttributesConfigWithStoreClass() {
        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setEnabled(true)
                .setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER)
                .setWriteDelaySeconds(10)
                .setClassName("className")
                .setWriteCoalescing(true)
                .setWriteBatchSize(500)
                .setProperty("key", "value");

        testMap(mapStoreConfig);
    }

    @Test
    public void testAttributesConfigWithStoreImplementation() {
        MapStoreConfig mapStoreConfig = new MapStoreConfig()
            .setEnabled(true)
            .setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER)
            .setWriteDelaySeconds(10)
            .setImplementation(new TestMapStore())
            .setWriteCoalescing(true)
            .setWriteBatchSize(500)
            .setProperty("key", "value");

        testMap(mapStoreConfig);
    }

    private static class TestMapStore implements MapStore {
        @Override
        public void store(Object key, Object value) { }

        @Override
        public void storeAll(Map map) { }

        @Override
        public void delete(Object key) { }

        @Override
        public void deleteAll(Collection keys) { }

        @Override
        public Object load(Object key) {
            return null;
        }

        @Override
        public Map loadAll(Collection keys) {
            return null;
        }

        @Override
        public Iterable loadAllKeys() {
            return null;
        }
    }

    @Test
    public void testCRDTReplication() {
        final CRDTReplicationConfig replicationConfig = new CRDTReplicationConfig()
                .setMaxConcurrentReplicationTargets(10)
                .setReplicationPeriodMillis(2000);
        final Config config = new Config().setCRDTReplicationConfig(replicationConfig);
        final Config xmlConfig = getNewConfigViaXMLGenerator(config);
        final CRDTReplicationConfig xmlReplicationConfig = xmlConfig.getCRDTReplicationConfig();

        assertNotNull(xmlReplicationConfig);
        assertEquals(10, xmlReplicationConfig.getMaxConcurrentReplicationTargets());
        assertEquals(2000, xmlReplicationConfig.getReplicationPeriodMillis());
    }

    @Test
    public void testAttributesConfigWithStoreFactory() {
        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setEnabled(true)
                .setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER)
                .setWriteDelaySeconds(10)
                .setWriteCoalescing(true)
                .setWriteBatchSize(500)
                .setFactoryClassName("factoryClassName")
                .setProperty("key", "value");

        testMap(mapStoreConfig);
    }

    @Test
    public void testAttributesConfigWithStoreFactoryImplementation() {
        MapStoreConfig mapStoreConfig = new MapStoreConfig()
            .setEnabled(true)
            .setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER)
            .setWriteDelaySeconds(10)
            .setWriteCoalescing(true)
            .setWriteBatchSize(500)
            .setFactoryImplementation((MapStoreFactory) (mapName, properties) -> null)
            .setProperty("key", "value");

        testMap(mapStoreConfig);
    }

    private void testMap(MapStoreConfig mapStoreConfig) {
        AttributeConfig attrConfig = new AttributeConfig()
                .setName("power")
                .setExtractorClassName("com.car.PowerExtractor");

        EvictionConfig evictionConfig1 = new EvictionConfig()
                .setSize(10)
                .setMaxSizePolicy(MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE);

        IndexConfig indexConfig = new IndexConfig().addAttribute("attribute").setType(IndexType.SORTED);

        EntryListenerConfig listenerConfig = new EntryListenerConfig("com.hazelcast.entrylistener", false, false);

        EvictionConfig evictionConfig2 = new EvictionConfig()
                .setMaxSizePolicy(MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE)
                .setSize(100)
                .setComparatorClassName("comparatorClassName")
                .setEvictionPolicy(EvictionPolicy.LRU);

        PredicateConfig predicateConfig1 = new PredicateConfig();
        predicateConfig1.setClassName("className");

        PredicateConfig predicateConfig2 = new PredicateConfig();
        predicateConfig2.setSql("sqlQuery");

        QueryCacheConfig queryCacheConfig1 = new QueryCacheConfig()
                .setName("queryCache1")
                .setPredicateConfig(predicateConfig1)
                .addEntryListenerConfig(listenerConfig)
                .setBatchSize(230)
                .setDelaySeconds(20)
                .setPopulate(false)
                .setBufferSize(8)
                .setInMemoryFormat(InMemoryFormat.BINARY)
                .setEvictionConfig(evictionConfig2)
                .setIncludeValue(false)
                .setCoalesce(false)
                .addIndexConfig(indexConfig);

        QueryCacheConfig queryCacheConfig2 = new QueryCacheConfig()
                .setName("queryCache2")
                .setPredicateConfig(predicateConfig2)
                .addEntryListenerConfig(listenerConfig)
                .setBatchSize(500)
                .setDelaySeconds(10)
                .setPopulate(true)
                .setBufferSize(10)
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setEvictionConfig(evictionConfig2)
                .setIncludeValue(true)
                .setCoalesce(true)
                .addIndexConfig(indexConfig);

        MapConfig expectedConfig = new MapConfig()
                .setName("carMap")
                .setEvictionConfig(evictionConfig1)
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .setMetadataPolicy(MetadataPolicy.CREATE_ON_UPDATE)
                .setMaxIdleSeconds(100)
                .setTimeToLiveSeconds(1000)
                .setCacheDeserializedValues(CacheDeserializedValues.ALWAYS)
                .setStatisticsEnabled(true)
                .setReadBackupData(true)
                .setBackupCount(2)
                .setAsyncBackupCount(3)
                .setMapStoreConfig(mapStoreConfig)
                .setWanReplicationRef(wanReplicationRef())
                .setPartitioningStrategyConfig(new PartitioningStrategyConfig("partitionStrategyClass"))
                .setMerkleTreeConfig(merkleTreeConfig())
                .setEventJournalConfig(eventJournalConfig())
                .setHotRestartConfig(hotRestartConfig())
                .addEntryListenerConfig(listenerConfig)
                .setIndexConfigs(singletonList(indexConfig))
                .addAttributeConfig(attrConfig)
                .setPartitionLostListenerConfigs(singletonList(
                        new MapPartitionLostListenerConfig("partitionLostListener"))
                );

        expectedConfig.setQueryCacheConfigs(asList(queryCacheConfig1, queryCacheConfig2));

        Config config = new Config()
                .addMapConfig(expectedConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        MapConfig actualConfig = xmlConfig.getMapConfig("carMap");
        AttributeConfig xmlAttrConfig = actualConfig.getAttributeConfigs().get(0);
        assertEquals(attrConfig.getName(), xmlAttrConfig.getName());
        assertEquals(attrConfig.getExtractorClassName(), xmlAttrConfig.getExtractorClassName());
        ConfigCompatibilityChecker.checkMapConfig(expectedConfig, actualConfig);
    }

    @Test
    public void testMapNearCacheConfig() {
        NearCacheConfig expectedConfig = new NearCacheConfig()
                .setName("nearCache")
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .setMaxIdleSeconds(42)
                .setCacheLocalEntries(true)
                .setInvalidateOnChange(true)
                .setLocalUpdatePolicy(NearCacheConfig.LocalUpdatePolicy.INVALIDATE)
                .setTimeToLiveSeconds(10)
                .setEvictionConfig(evictionConfig())
                .setSerializeKeys(true);

        MapConfig mapConfig = new MapConfig()
                .setName("nearCacheTest")
                .setNearCacheConfig(expectedConfig);

        Config config = new Config()
                .addMapConfig(mapConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        NearCacheConfig actualConfig = xmlConfig.getMapConfig("nearCacheTest").getNearCacheConfig();
        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testMapNearCacheEvictionConfig() {
        NearCacheConfig expectedConfig = new NearCacheConfig()
                .setName("nearCache");
        expectedConfig.getEvictionConfig().setSize(23).setEvictionPolicy(EvictionPolicy.LRU);

        MapConfig mapConfig = new MapConfig()
                .setName("nearCacheTest")
                .setNearCacheConfig(expectedConfig);

        Config config = new Config()
                .addMapConfig(mapConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        NearCacheConfig actualConfig = xmlConfig.getMapConfig("nearCacheTest").getNearCacheConfig();
        assertEquals(23, actualConfig.getEvictionConfig().getSize());
        assertEquals("LRU", actualConfig.getEvictionConfig().getEvictionPolicy().name());
        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testMultiMapConfig() {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig()
                .setPolicy(DiscardMergePolicy.class.getSimpleName())
                .setBatchSize(2342);

        MultiMapConfig multiMapConfig = new MultiMapConfig()
                .setName("myMultiMap")
                .setBackupCount(2)
                .setAsyncBackupCount(3)
                .setBinary(false)
                .setMergePolicyConfig(mergePolicyConfig);

        Config config = new Config().addMultiMapConfig(multiMapConfig);
        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        assertEquals(multiMapConfig, xmlConfig.getMultiMapConfig("myMultiMap"));
    }

    @Test
    public void testWanConfig() {
        @SuppressWarnings("rawtypes")
        HashMap<String, Comparable> props = new HashMap<>();
        props.put("prop1", "val1");
        props.put("prop2", "val2");
        props.put("prop3", "val3");
        WanReplicationConfig wanReplicationConfig = new WanReplicationConfig()
                .setName("testName")
                .setConsumerConfig(new WanConsumerConfig().setClassName("dummyClass").setProperties(props));
        WanBatchPublisherConfig batchPublisher = new WanBatchPublisherConfig()
                .setClusterName("dummyGroup")
                .setPublisherId("dummyPublisherId")
                .setSnapshotEnabled(false)
                .setInitialPublisherState(WanPublisherState.STOPPED)
                .setQueueCapacity(1000)
                .setBatchSize(500)
                .setBatchMaxDelayMillis(1000)
                .setResponseTimeoutMillis(60000)
                .setQueueFullBehavior(WanQueueFullBehavior.DISCARD_AFTER_MUTATION)
                .setAcknowledgeType(WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE)
                .setDiscoveryPeriodSeconds(20)
                .setMaxTargetEndpoints(100)
                .setMaxConcurrentInvocations(500)
                .setUseEndpointPrivateAddress(true)
                .setIdleMinParkNs(100)
                .setIdleMaxParkNs(1000)
                .setTargetEndpoints("a,b,c,d")
                .setAwsConfig(getDummyAwsConfig())
                .setDiscoveryConfig(getDummyDiscoveryConfig())
                .setEndpoint("WAN")
                .setProperties(props);

        batchPublisher.getSyncConfig()
                .setConsistencyCheckStrategy(ConsistencyCheckStrategy.MERKLE_TREES);

        WanCustomPublisherConfig customPublisher = new WanCustomPublisherConfig()
                .setPublisherId("dummyPublisherId")
                .setClassName("className")
                .setProperties(props);

        WanConsumerConfig wanConsumerConfig = new WanConsumerConfig()
                .setClassName("dummyClass")
                .setProperties(props)
                .setPersistWanReplicatedData(false);

        wanReplicationConfig.setConsumerConfig(wanConsumerConfig)
                            .addBatchReplicationPublisherConfig(batchPublisher)
                            .addCustomPublisherConfig(customPublisher);

        Config config = new Config().addWanReplicationConfig(wanReplicationConfig);
        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        ConfigCompatibilityChecker.checkWanConfigs(
                config.getWanReplicationConfigs(),
                xmlConfig.getWanReplicationConfigs());
    }

    @Test
    public void testCardinalityEstimator() {
        Config cfg = new Config();
        CardinalityEstimatorConfig estimatorConfig = new CardinalityEstimatorConfig()
                .setBackupCount(2)
                .setAsyncBackupCount(3)
                .setName("Existing")
                .setSplitBrainProtectionName("splitBrainProtection")
                .setMergePolicyConfig(new MergePolicyConfig("DiscardMergePolicy", 14));
        cfg.addCardinalityEstimatorConfig(estimatorConfig);

        CardinalityEstimatorConfig defaultCardinalityEstConfig = new CardinalityEstimatorConfig();
        cfg.addCardinalityEstimatorConfig(defaultCardinalityEstConfig);

        CardinalityEstimatorConfig existing = getNewConfigViaXMLGenerator(cfg).getCardinalityEstimatorConfig("Existing");
        assertEquals(estimatorConfig, existing);

        CardinalityEstimatorConfig fallsbackToDefault = getNewConfigViaXMLGenerator(cfg)
                .getCardinalityEstimatorConfig("NotExisting/Default");
        assertEquals(defaultCardinalityEstConfig.getMergePolicyConfig(), fallsbackToDefault.getMergePolicyConfig());
        assertEquals(defaultCardinalityEstConfig.getBackupCount(), fallsbackToDefault.getBackupCount());
        assertEquals(defaultCardinalityEstConfig.getAsyncBackupCount(), fallsbackToDefault.getAsyncBackupCount());
        assertEquals(defaultCardinalityEstConfig.getSplitBrainProtectionName(), fallsbackToDefault.getSplitBrainProtectionName());
    }

    @Test
    public void testTopicGlobalOrdered() {
        Config cfg = new Config();

        TopicConfig expectedConfig = new TopicConfig()
                .setName("TestTopic")
                .setGlobalOrderingEnabled(true)
                .setStatisticsEnabled(true)
                .setMessageListenerConfigs(singletonList(new ListenerConfig("foo.bar.Listener")));
        cfg.addTopicConfig(expectedConfig);

        TopicConfig actualConfig = getNewConfigViaXMLGenerator(cfg).getTopicConfig("TestTopic");

        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testTopicMultiThreaded() {
        String testTopic = "TestTopic";
        Config cfg = new Config();

        TopicConfig expectedConfig = new TopicConfig()
                .setName(testTopic)
                .setMultiThreadingEnabled(true)
                .setStatisticsEnabled(true)
                .setMessageListenerConfigs(singletonList(new ListenerConfig("foo.bar.Listener")));
        cfg.addTopicConfig(expectedConfig);

        TopicConfig actualConfig = getNewConfigViaXMLGenerator(cfg).getTopicConfig(testTopic);

        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testReliableTopic() {
        String testTopic = "TestTopic";
        Config cfg = new Config();

        ReliableTopicConfig expectedConfig = new ReliableTopicConfig()
                .setName(testTopic)
                .setReadBatchSize(10)
                .setTopicOverloadPolicy(TopicOverloadPolicy.BLOCK)
                .setStatisticsEnabled(true)
                .setMessageListenerConfigs(singletonList(new ListenerConfig("foo.bar.Listener")));

        cfg.addReliableTopicConfig(expectedConfig);

        ReliableTopicConfig actualConfig = getNewConfigViaXMLGenerator(cfg).getReliableTopicConfig(testTopic);

        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testScheduledExecutor() {
        Config cfg = new Config();
        ScheduledExecutorConfig scheduledExecutorConfig =
                new ScheduledExecutorConfig()
                        .setCapacity(1)
                        .setCapacityPolicy(ScheduledExecutorConfig.CapacityPolicy.PER_PARTITION)
                        .setDurability(2)
                        .setName("Existing")
                        .setPoolSize(3)
                        .setSplitBrainProtectionName("splitBrainProtection")
                        .setMergePolicyConfig(new MergePolicyConfig("JediPolicy", 23));
        cfg.addScheduledExecutorConfig(scheduledExecutorConfig);

        ScheduledExecutorConfig defaultSchedExecConfig = new ScheduledExecutorConfig();
        cfg.addScheduledExecutorConfig(defaultSchedExecConfig);

        ScheduledExecutorConfig existing = getNewConfigViaXMLGenerator(cfg).getScheduledExecutorConfig("Existing");
        assertEquals(scheduledExecutorConfig, existing);

        ScheduledExecutorConfig fallsbackToDefault = getNewConfigViaXMLGenerator(cfg)
                .getScheduledExecutorConfig("NotExisting/Default");
        assertEquals(defaultSchedExecConfig.getMergePolicyConfig(), fallsbackToDefault.getMergePolicyConfig());
        assertEquals(defaultSchedExecConfig.getCapacity(), fallsbackToDefault.getCapacity());
        assertEquals(defaultSchedExecConfig.getCapacityPolicy(), fallsbackToDefault.getCapacityPolicy());
        assertEquals(defaultSchedExecConfig.getPoolSize(), fallsbackToDefault.getPoolSize());
        assertEquals(defaultSchedExecConfig.getDurability(), fallsbackToDefault.getDurability());
    }

    @Test
    public void testSplitBrainProtectionConfig_configByClassName() {
        Config config = new Config();
        SplitBrainProtectionConfig splitBrainProtectionConfig = new SplitBrainProtectionConfig("test-splitBrainProtection", true, 3);
        splitBrainProtectionConfig.setProtectOn(SplitBrainProtectionOn.READ_WRITE)
                .setFunctionClassName("com.hazelcast.SplitBrainProtectionFunction");
        config.addSplitBrainProtectionConfig(splitBrainProtectionConfig);

        SplitBrainProtectionConfig generatedConfig = getNewConfigViaXMLGenerator(config).
                getSplitBrainProtectionConfig("test-splitBrainProtection");
        assertTrue(generatedConfig.toString() + " should be compatible with " + splitBrainProtectionConfig.toString(),
                new SplitBrainProtectionConfigChecker().check(splitBrainProtectionConfig, generatedConfig));
    }

    @Test
    public void testConfig_configuredByRecentlyActiveSplitBrainProtectionConfigBuilder() {
        Config config = new Config();
        SplitBrainProtectionConfig splitBrainProtectionConfig = SplitBrainProtectionConfig.newRecentlyActiveSplitBrainProtectionConfigBuilder("recently-active", 3, 3141592)
                .build();
        splitBrainProtectionConfig.setProtectOn(SplitBrainProtectionOn.READ_WRITE)
                .addListenerConfig(new SplitBrainProtectionListenerConfig("com.hazelcast.SplitBrainProtectionListener"));
        config.addSplitBrainProtectionConfig(splitBrainProtectionConfig);

        SplitBrainProtectionConfig generatedConfig = getNewConfigViaXMLGenerator(config).getSplitBrainProtectionConfig("recently-active");
        assertTrue(generatedConfig.toString() + " should be compatible with " + splitBrainProtectionConfig.toString(),
                new SplitBrainProtectionConfigChecker().check(splitBrainProtectionConfig, generatedConfig));
    }

    @Test
    public void testConfig_configuredByProbabilisticSplitBrainProtectionConfigBuilder() {
        Config config = new Config();
        SplitBrainProtectionConfig splitBrainProtectionConfig = SplitBrainProtectionConfig.newProbabilisticSplitBrainProtectionConfigBuilder("probabilistic-split-brain-protection", 3)
                .withHeartbeatIntervalMillis(1)
                .withAcceptableHeartbeatPauseMillis(2)
                .withMaxSampleSize(3)
                .withMinStdDeviationMillis(4)
                .withSuspicionThreshold(5)
                .build();
        splitBrainProtectionConfig.setProtectOn(SplitBrainProtectionOn.READ_WRITE)
                .addListenerConfig(new SplitBrainProtectionListenerConfig("com.hazelcast.SplitBrainProtectionListener"));
        config.addSplitBrainProtectionConfig(splitBrainProtectionConfig);

        SplitBrainProtectionConfig generatedConfig = getNewConfigViaXMLGenerator(config).getSplitBrainProtectionConfig("probabilistic-split-brain-protection");
        assertTrue(generatedConfig.toString() + " should be compatible with " + splitBrainProtectionConfig.toString(),
                new SplitBrainProtectionConfigChecker().check(splitBrainProtectionConfig, generatedConfig));
    }

    @Test
    public void testCPSubsystemConfig() {
        Config config = new Config();

        config.getCPSubsystemConfig()
                .setCPMemberCount(10)
                .setGroupSize(5)
                .setSessionTimeToLiveSeconds(15)
                .setSessionHeartbeatIntervalSeconds(3)
                .setMissingCPMemberAutoRemovalSeconds(120)
                .setFailOnIndeterminateOperationState(true)
                .setPersistenceEnabled(true)
                .setBaseDir(new File("/custom-dir"));

        config.getCPSubsystemConfig()
                .getRaftAlgorithmConfig()
                .setLeaderElectionTimeoutInMillis(500)
                .setLeaderHeartbeatPeriodInMillis(100)
                .setMaxMissedLeaderHeartbeatCount(10)
                .setAppendRequestMaxEntryCount(25)
                .setAppendRequestMaxEntryCount(250)
                .setUncommittedEntryCountToRejectNewAppends(75)
                .setAppendRequestBackoffTimeoutInMillis(50);

        config.getCPSubsystemConfig()
                .addSemaphoreConfig(new SemaphoreConfig("sem1", true, 1))
                .addSemaphoreConfig(new SemaphoreConfig("sem2", false, 2));

        config.getCPSubsystemConfig()
                .addLockConfig(new FencedLockConfig("lock1", 1))
                .addLockConfig(new FencedLockConfig("lock1", 2));


        CPSubsystemConfig generatedConfig = getNewConfigViaXMLGenerator(config).getCPSubsystemConfig();
        assertTrue(generatedConfig + " should be compatible with " + config.getCPSubsystemConfig(),
                new CPSubsystemConfigChecker().check(config.getCPSubsystemConfig(), generatedConfig));
    }

    @Test
    public void testMetricsConfig() {
        Config config = new Config();

        config.getMetricsConfig()
              .setEnabled(false)
              .setCollectionFrequencySeconds(10);

        config.getMetricsConfig().getManagementCenterConfig()
              .setEnabled(false)
              .setRetentionSeconds(11);

        config.getMetricsConfig().getJmxConfig()
              .setEnabled(false);

        MetricsConfig generatedConfig = getNewConfigViaXMLGenerator(config).getMetricsConfig();
        assertTrue(generatedConfig + " should be compatible with " + config.getMetricsConfig(),
                new MetricsConfigChecker().check(config.getMetricsConfig(), generatedConfig));
    }

    @Test
    public void testSqlConfig() {
        Config confiig = new Config();

        confiig.getSqlConfig().setExecutorPoolSize(10);
        confiig.getSqlConfig().setOperationPoolSize(20);
        confiig.getSqlConfig().setQueryTimeoutMillis(30L);

        SqlConfig generatedConfig = getNewConfigViaXMLGenerator(confiig).getSqlConfig();

        assertEquals(confiig.getSqlConfig().getExecutorPoolSize(), generatedConfig.getExecutorPoolSize());
        assertEquals(confiig.getSqlConfig().getOperationPoolSize(), generatedConfig.getOperationPoolSize());
        assertEquals(confiig.getSqlConfig().getQueryTimeoutMillis(), generatedConfig.getQueryTimeoutMillis());
    }

    @Test
    public void testMemcacheProtocolConfig() {
        MemcacheProtocolConfig memcacheProtocolConfig = new MemcacheProtocolConfig().setEnabled(true);
        Config config = new Config();
        config.getNetworkConfig().setMemcacheProtocolConfig(memcacheProtocolConfig);
        MemcacheProtocolConfig generatedConfig = getNewConfigViaXMLGenerator(config).getNetworkConfig().getMemcacheProtocolConfig();
        assertTrue(generatedConfig.toString() + " should be compatible with " + memcacheProtocolConfig.toString(),
                new ConfigCompatibilityChecker.MemcacheProtocolConfigChecker().check(memcacheProtocolConfig, generatedConfig));
    }

    @Test
    public void testEmptyRestApiConfig() {
        RestApiConfig restApiConfig = new RestApiConfig();
        Config config = new Config();
        config.getNetworkConfig().setRestApiConfig(restApiConfig);
        RestApiConfig generatedConfig = getNewConfigViaXMLGenerator(config).getNetworkConfig().getRestApiConfig();
        assertTrue(generatedConfig.toString() + " should be compatible with " + restApiConfig.toString(),
                new ConfigCompatibilityChecker.RestApiConfigChecker().check(restApiConfig, generatedConfig));
    }

    @Test
    public void testAllEnabledRestApiConfig() {
        RestApiConfig restApiConfig = new RestApiConfig();
        restApiConfig.setEnabled(true).enableAllGroups();
        Config config = new Config();
        config.getNetworkConfig().setRestApiConfig(restApiConfig);
        RestApiConfig generatedConfig = getNewConfigViaXMLGenerator(config).getNetworkConfig().getRestApiConfig();
        assertTrue(generatedConfig.toString() + " should be compatible with " + restApiConfig.toString(),
                new ConfigCompatibilityChecker.RestApiConfigChecker().check(restApiConfig, generatedConfig));
    }

    @Test
    public void testExplicitlyAssignedGroupsRestApiConfig() {
        RestApiConfig restApiConfig = new RestApiConfig();
        restApiConfig.setEnabled(true);
        restApiConfig.enableGroups(RestEndpointGroup.CLUSTER_READ, RestEndpointGroup.HEALTH_CHECK, RestEndpointGroup.HOT_RESTART,
                RestEndpointGroup.WAN);
        restApiConfig.disableGroups(RestEndpointGroup.CLUSTER_WRITE, RestEndpointGroup.DATA);
        Config config = new Config();
        config.getNetworkConfig().setRestApiConfig(restApiConfig);
        RestApiConfig generatedConfig = getNewConfigViaXMLGenerator(config).getNetworkConfig().getRestApiConfig();
        assertTrue(generatedConfig.toString() + " should be compatible with " + restApiConfig.toString(),
                new ConfigCompatibilityChecker.RestApiConfigChecker().check(restApiConfig, generatedConfig));
    }

    @Test
    public void testAdvancedNetworkMulticastJoinConfig() {
        Config cfg = new Config();
        cfg.getAdvancedNetworkConfig().setEnabled(true);
        MulticastConfig expectedConfig = multicastConfig();

        cfg.getAdvancedNetworkConfig().getJoin().setMulticastConfig(expectedConfig);
        cfg.getAdvancedNetworkConfig().setEnabled(true);

        MulticastConfig actualConfig = getNewConfigViaXMLGenerator(cfg)
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

        TcpIpConfig actualConfig = getNewConfigViaXMLGenerator(cfg)
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

        Config newConfigViaXMLGenerator = getNewConfigViaXMLGenerator(cfg);
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

        Config newConfigViaXMLGenerator = getNewConfigViaXMLGenerator(cfg);
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

        EndpointConfig actual = getNewConfigViaXMLGenerator(cfg)
                .getAdvancedNetworkConfig().getEndpointConfigs().get(expected.getQualifier());

        checkEndpointConfigCompatible(expected, actual);
    }

    @Test
    public void testEndpointConfig_defaultConfiguration() {
        Config cfg = new Config();

        ServerSocketEndpointConfig expected = new ServerSocketEndpointConfig();
        expected.setProtocolType(MEMBER);

        cfg.getAdvancedNetworkConfig().setEnabled(true);
        cfg.getAdvancedNetworkConfig().setMemberEndpointConfig(expected);

        EndpointConfig actual = getNewConfigViaXMLGenerator(cfg)
                .getAdvancedNetworkConfig().getEndpointConfigs().get(expected.getQualifier());

        checkEndpointConfigCompatible(expected, actual);
    }

    @Test
    public void testUserCodeDeployment() {
        Config config = new Config();

        UserCodeDeploymentConfig expected = new UserCodeDeploymentConfig();
        expected.setEnabled(true)
                .setBlacklistedPrefixes("some-prefixes")
                .setClassCacheMode(UserCodeDeploymentConfig.ClassCacheMode.ETERNAL)
                .setProviderFilter("HAS_ATTRIBUTE:class-provider")
                .setWhitelistedPrefixes("other-prefixes")
                .setProviderMode(UserCodeDeploymentConfig.ProviderMode.LOCAL_AND_CACHED_CLASSES);
        config.setUserCodeDeploymentConfig(expected);

        Config newConfigViaXMLGenerator = getNewConfigViaXMLGenerator(config);
        UserCodeDeploymentConfig actual = newConfigViaXMLGenerator.getUserCodeDeploymentConfig();

        assertEquals(expected.isEnabled(), actual.isEnabled());
        assertEquals(expected.getBlacklistedPrefixes(), actual.getBlacklistedPrefixes());
        assertEquals(expected.getClassCacheMode(), actual.getClassCacheMode());
        assertEquals(expected.getProviderFilter(), actual.getProviderFilter());
        assertEquals(expected.getWhitelistedPrefixes(), actual.getWhitelistedPrefixes());
        assertEquals(expected.getProviderMode(), actual.getProviderMode());
    }

    private DiscoveryConfig getDummyDiscoveryConfig() {
        DiscoveryStrategyConfig strategyConfig = new DiscoveryStrategyConfig(new TestDiscoveryStrategyFactory());
        strategyConfig.addProperty("prop1", "val1");
        strategyConfig.addProperty("prop2", "val2");

        DiscoveryConfig discoveryConfig = new DiscoveryConfig();
        discoveryConfig.setNodeFilter(candidate -> false);
        assert discoveryConfig.getNodeFilterClass() == null;
        assert discoveryConfig.getNodeFilter() != null;
        discoveryConfig.addDiscoveryStrategyConfig(strategyConfig);
        discoveryConfig.addDiscoveryStrategyConfig(new DiscoveryStrategyConfig("dummyClass2"));

        return discoveryConfig;
    }

    private static class TestDiscoveryStrategyFactory implements DiscoveryStrategyFactory {
        @Override
        public Class<? extends DiscoveryStrategy> getDiscoveryStrategyType() {
            return null;
        }

        @Override
        public DiscoveryStrategy newDiscoveryStrategy(DiscoveryNode discoveryNode, ILogger logger, Map<String, Comparable> properties) {
            return null;
        }

        @Override
        public Collection<PropertyDefinition> getConfigurationProperties() {
            return null;
        }
    }

    private AwsConfig getDummyAwsConfig() {
        return new AwsConfig().setProperty("host-header", "dummyHost")
                .setProperty("region", "dummyRegion")
                .setEnabled(false)
                .setProperty("connection-timeout-seconds", "1")
                .setProperty("access-key", "dummyKey")
                .setProperty("iam-role", "dummyIam")
                .setProperty("secret-key", "dummySecretKey")
                .setProperty("security-group-name", "dummyGroupName")
                .setProperty("tag-key", "dummyTagKey")
                .setProperty("tag-value", "dummyTagValue");
    }

    private static Config getNewConfigViaXMLGenerator(Config config) {
        return getNewConfigViaXMLGenerator(config, true);
    }

    private static Config getNewConfigViaXMLGenerator(Config config, boolean maskSensitiveFields) {
        ConfigXmlGenerator configXmlGenerator = new ConfigXmlGenerator(true, maskSensitiveFields);
        String xml = configXmlGenerator.generate(config);
        System.err.println("XML: " + xml);
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        return configBuilder.build();
    }

    private static WanReplicationRef wanReplicationRef() {
        return new WanReplicationRef()
                .setName("wanReplication")
                .setMergePolicyClassName("mergePolicy")
                .setRepublishingEnabled(true)
                .setFilters(Arrays.asList("filter1", "filter2"));
    }

    private static HotRestartConfig hotRestartConfig() {
        return new HotRestartConfig()
                .setEnabled(true)
                .setFsync(true);
    }

    private static MerkleTreeConfig merkleTreeConfig() {
        return new MerkleTreeConfig()
                .setEnabled(true)
                .setDepth(15);
    }

    private static EventJournalConfig eventJournalConfig() {
        return new EventJournalConfig()
                .setEnabled(true)
                .setCapacity(123)
                .setTimeToLiveSeconds(321);
    }

    private static EvictionConfig evictionConfig() {
        return new EvictionConfig()
                .setEvictionPolicy(EvictionPolicy.LRU)
                .setComparatorClassName("comparatorClassName")
                .setSize(10)
                .setMaxSizePolicy(MaxSizePolicy.ENTRY_COUNT);
    }

    private static TcpIpConfig tcpIpConfig() {
        return new TcpIpConfig()
                .setEnabled(true)
                .setConnectionTimeoutSeconds(10)
                .addMember("10.11.12.1,10.11.12.2")
                .setRequiredMember("10.11.11.2");
    }

    private static MulticastConfig multicastConfig() {
        return new MulticastConfig()
                .setEnabled(true)
                .setMulticastTimeoutSeconds(10)
                .setLoopbackModeEnabled(true)
                .setMulticastGroup("224.2.2.3")
                .setMulticastTimeToLive(42)
                .setMulticastPort(4242)
                .setTrustedInterfaces(newHashSet("*"));
    }

    private static void assertFailureDetectorConfigEquals(IcmpFailureDetectorConfig expected,
                                                          IcmpFailureDetectorConfig actual) {
        assertEquals(expected.isEnabled(), actual.isEnabled());
        assertEquals(expected.getIntervalMilliseconds(), actual.getIntervalMilliseconds());
        assertEquals(expected.getTimeoutMilliseconds(), actual.getTimeoutMilliseconds());
        assertEquals(expected.getTtl(), actual.getTtl());
        assertEquals(expected.getMaxAttempts(), actual.getMaxAttempts());
        assertEquals(expected.isFailFastOnStartup(), actual.isFailFastOnStartup());
        assertEquals(expected.isParallelMode(), actual.isParallelMode());
        assertEquals(expected, actual);
    }
}
