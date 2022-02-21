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

package com.hazelcast.config;

import com.hazelcast.config.ConfigCompatibilityChecker.CPSubsystemConfigChecker;
import com.hazelcast.config.ConfigCompatibilityChecker.InstanceTrackingConfigChecker;
import com.hazelcast.config.ConfigCompatibilityChecker.MetricsConfigChecker;
import com.hazelcast.config.ConfigCompatibilityChecker.SplitBrainProtectionConfigChecker;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.config.cp.FencedLockConfig;
import com.hazelcast.config.cp.SemaphoreConfig;
import com.hazelcast.config.security.JaasAuthenticationConfig;
import com.hazelcast.config.security.KerberosAuthenticationConfig;
import com.hazelcast.config.security.KerberosIdentityConfig;
import com.hazelcast.config.security.LdapAuthenticationConfig;
import com.hazelcast.config.security.LdapRoleMappingMode;
import com.hazelcast.config.security.LdapSearchScope;
import com.hazelcast.config.security.RealmConfig;
import com.hazelcast.config.security.SimpleAuthenticationConfig;
import com.hazelcast.config.security.TlsAuthenticationConfig;
import com.hazelcast.config.security.TokenEncoding;
import com.hazelcast.config.security.TokenIdentityConfig;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.util.TriTuple;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.memory.Capacity;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.spi.MemberAddressProvider;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionOn;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import example.serialization.EmployeeDTO;
import example.serialization.EmployeeDTOSerializer;
import example.serialization.EmployerDTO;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.EventListener;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.google.common.collect.Sets.newHashSet;
import static com.hazelcast.config.ConfigCompatibilityChecker.checkEndpointConfigCompatible;
import static com.hazelcast.config.ConfigXmlGenerator.MASK_FOR_SENSITIVE_DATA;
import static com.hazelcast.config.HotRestartClusterDataRecoveryPolicy.FULL_RECOVERY_ONLY;
import static com.hazelcast.instance.ProtocolType.MEMBER;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

// Please also take a look at the DynamicConfigXmlGeneratorTest.
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ConfigXmlGeneratorTest extends HazelcastTestSupport {

    private static final ILogger LOGGER = Logger.getLogger(ConfigXmlGeneratorTest.class);

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
    public void testNetworkAutoDetectionJoinConfig() {
        Config cfg = new Config();
        cfg.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        Config actualConfig = getNewConfigViaXMLGenerator(cfg);
        assertFalse(actualConfig.getNetworkConfig().getJoin().getAutoDetectionConfig().isEnabled());
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
        public void init(Properties properties) {
        }

        @Override
        public void onConnect(Socket connectedSocket) throws IOException {
        }
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

    private static class TestEventListener implements EventListener {
    }

    @Test
    public void testPersistenceConfig() {
        Config cfg = new Config();

        PersistenceConfig expectedConfig = cfg.getPersistenceConfig();
        expectedConfig.setEnabled(true)
                .setClusterDataRecoveryPolicy(PersistenceClusterDataRecoveryPolicy.FULL_RECOVERY_ONLY)
                .setValidationTimeoutSeconds(100)
                .setDataLoadTimeoutSeconds(130)
                .setRebalanceDelaySeconds(240)
                .setBaseDir(new File("nonExisting-base").getAbsoluteFile())
                .setBackupDir(new File("nonExisting-backup").getAbsoluteFile())
                .setParallelism(5).setAutoRemoveStaleData(false);

        PersistenceConfig actualConfig = getNewConfigViaXMLGenerator(cfg, false).getPersistenceConfig();

        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testDynamicConfigurationConfig() {
        DynamicConfigurationConfig dynamicConfigurationConfig = new DynamicConfigurationConfig()
                .setPersistenceEnabled(true)
                .setBackupDir(new File("backup-dir").getAbsoluteFile())
                .setBackupCount(7);

        Config config = new Config().setDynamicConfigurationConfig(dynamicConfigurationConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        ConfigCompatibilityChecker.checkDynamicConfigurationConfig(dynamicConfigurationConfig, xmlConfig.getDynamicConfigurationConfig());
    }

    @Test
    public void testDeviceConfig() {
        LocalDeviceConfig localDeviceConfig0 = new LocalDeviceConfig()
                .setName("null-device")
                .setBaseDir(new File("null-dir").getAbsoluteFile())
                .setCapacity(Capacity.of(6522, MemoryUnit.MEGABYTES))
                .setBlockSize(512)
                .setReadIOThreadCount(100)
                .setWriteIOThreadCount(100);

        LocalDeviceConfig localDeviceConfig1 = new LocalDeviceConfig()
                .setName("local-device")
                .setBaseDir(new File("local-dir").getAbsoluteFile())
                .setCapacity(Capacity.of(198719826236L, MemoryUnit.KILOBYTES))
                .setBlockSize(1024)
                .setReadIOThreadCount(200)
                .setWriteIOThreadCount(200);

        Config config = new Config()
                .addDeviceConfig(localDeviceConfig0)
                .addDeviceConfig(localDeviceConfig1);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        ConfigCompatibilityChecker.checkDeviceConfig(localDeviceConfig0, xmlConfig.getDeviceConfig("null-device"));
        ConfigCompatibilityChecker.checkDeviceConfig(localDeviceConfig1, xmlConfig.getDeviceConfig("local-device"));
    }

    @Test
    public void testHotRestartConfig_equalsToPersistenceConfig() {
        Config cfg = new Config();

        HotRestartPersistenceConfig expectedConfig = cfg.getHotRestartPersistenceConfig();
        expectedConfig.setEnabled(true)
                .setClusterDataRecoveryPolicy(FULL_RECOVERY_ONLY)
                .setValidationTimeoutSeconds(100)
                .setDataLoadTimeoutSeconds(130)
                .setBaseDir(new File("nonExisting-base").getAbsoluteFile())
                .setBackupDir(new File("nonExisting-backup").getAbsoluteFile())
                .setParallelism(5).setAutoRemoveStaleData(false);

        Config actualConfig = getNewConfigViaXMLGenerator(cfg, false);

        assertEquals(cfg.getHotRestartPersistenceConfig(), actualConfig.getHotRestartPersistenceConfig());
        assertEquals(cfg.getPersistenceConfig(), actualConfig.getPersistenceConfig());
    }

    private void configurePersistence(Config cfg) {
        cfg.getPersistenceConfig()
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
    public void testPersistenceEncryptionAtRestConfig_whenJavaKeyStore_andMaskingDisabled() {
        Config cfg = new Config();

        configurePersistence(cfg);

        PersistenceConfig expectedConfig = cfg.getPersistenceConfig();

        PersistenceConfig actualConfig = getNewConfigViaXMLGenerator(cfg, false).getPersistenceConfig();

        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testPersistenceEncryptionAtRestConfig_whenJavaKeyStore_andMaskingEnabled() {
        Config cfg = new Config();

        configurePersistence(cfg);

        PersistenceConfig hrConfig = getNewConfigViaXMLGenerator(cfg).getPersistenceConfig();

        EncryptionAtRestConfig actualConfig = hrConfig.getEncryptionAtRestConfig();
        assertTrue(actualConfig.getSecureStoreConfig() instanceof JavaKeyStoreSecureStoreConfig);
        JavaKeyStoreSecureStoreConfig keyStoreConfig = (JavaKeyStoreSecureStoreConfig) actualConfig.getSecureStoreConfig();
        assertEquals(MASK_FOR_SENSITIVE_DATA, keyStoreConfig.getPassword());
    }

    @Test
    public void testPersistenceEncryptionAtRestConfig_whenVault_andMaskingEnabled() {
        Config cfg = new Config();

        PersistenceConfig expectedConfig = cfg.getPersistenceConfig();
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

        PersistenceConfig persistenceConfig = getNewConfigViaXMLGenerator(cfg).getPersistenceConfig();

        EncryptionAtRestConfig actualConfig = persistenceConfig.getEncryptionAtRestConfig();
        assertTrue(actualConfig.getSecureStoreConfig() instanceof VaultSecureStoreConfig);
        VaultSecureStoreConfig vaultConfig = (VaultSecureStoreConfig) actualConfig.getSecureStoreConfig();
        assertEquals(MASK_FOR_SENSITIVE_DATA, vaultConfig.getToken());
        assertEquals(MASK_FOR_SENSITIVE_DATA, vaultConfig.getSSLConfig().getProperty("keyStorePassword"));
        assertEquals(MASK_FOR_SENSITIVE_DATA, vaultConfig.getSSLConfig().getProperty("trustStorePassword"));
    }

    @Test
    public void testPersistenceEncryptionAtRestConfig_whenVault_andMaskingDisabled() {
        Config cfg = new Config();

        PersistenceConfig expectedConfig = cfg.getPersistenceConfig();
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

        PersistenceConfig actualConfig = getNewConfigViaXMLGenerator(cfg, false).getPersistenceConfig();

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
                .setClientPermissionConfigs(new HashSet<>(asList(
                        new PermissionConfig()
                                .setActions(newHashSet("read", "remove"))
                                .setEndpoints(newHashSet("127.0.0.1", "127.0.0.2"))
                                .setType(PermissionConfig.PermissionType.ATOMIC_LONG)
                                .setName("mycounter")
                                .setPrincipal("devos"),
                        new PermissionConfig()
                                .setType(PermissionConfig.PermissionType.MANAGEMENT)
                                .setPrincipal("mcadmin"),
                        new PermissionConfig()
                                .setType(PermissionConfig.PermissionType.CONFIG),
                        new PermissionConfig()
                                .setActions(newHashSet("read", "create"))
                                .setType(PermissionConfig.PermissionType.REPLICATEDMAP)
                                .setName("rmap")
                                .setPrincipal("monitor")
                )));

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
                        .setKeytabFile("/opt/test.keytab")
                        .setPrincipal("testPrincipal")
                        .setLdapAuthenticationConfig(new LdapAuthenticationConfig()
                                .setUrl("url")))
                .setKerberosIdentityConfig(new KerberosIdentityConfig()
                        .setRealm("HAZELCAST.COM")
                        .setSecurityRealm("krb5Init")
                        .setKeytabFile("/opt/test.keytab")
                        .setPrincipal("testPrincipal")
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
    public void testSimpleAuthenticationConfig() {
        Config cfg = new Config();
        RealmConfig realmConfig = new RealmConfig().setSimpleAuthenticationConfig(new SimpleAuthenticationConfig()
                .setRoleSeparator(":")
                .addUser("test", "1234", "monitor", "hazelcast")
                .addUser("dev", "secret", "root")
        );
        SecurityConfig expectedConfig = new SecurityConfig().setMemberRealmConfig("simpleRealm", realmConfig);
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
                .setAllowOverrideDefaultSerializers(true)
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
        assertEquals(expectedConfig.isAllowOverrideDefaultSerializers(), actualConfig.isAllowOverrideDefaultSerializers());
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

    @Test
    public void testCompactSerialization() {
        Config config = new Config();

        CompactSerializationConfig expected = new CompactSerializationConfig();
        expected.setEnabled(true);
        expected.register(EmployerDTO.class);
        expected.register(EmployeeDTO.class, "employee", new EmployeeDTOSerializer());

        config.getSerializationConfig().setCompactSerializationConfig(expected);

        CompactSerializationConfig actual = getNewConfigViaXMLGenerator(config).getSerializationConfig().getCompactSerializationConfig();
        assertEquals(expected.isEnabled(), actual.isEnabled());

        // Since we don't have APIs of the form register(String) or register(String, String, String) in the
        // compact serialization config, when we read the config from XML/YAML, we store registered classes
        // in a different map.
        Map<String, TriTuple<String, String, String>> namedRegistries
                = CompactSerializationConfigAccessor.getNamedRegistrations(actual);

        Map<String, TriTuple<Class, String, CompactSerializer>> registrations
                = CompactSerializationConfigAccessor.getRegistrations(actual);

        for (Map.Entry<String, TriTuple<Class, String, CompactSerializer>> entry : registrations.entrySet()) {
            String key = entry.getKey();
            TriTuple<Class, String, CompactSerializer> expectedRegistration = entry.getValue();
            TriTuple<String, String, String> actualRegistration = namedRegistries.get(key);

            assertEquals(expectedRegistration.element1.getName(), actualRegistration.element1);
            assertEquals(expectedRegistration.element2, actualRegistration.element2);

            CompactSerializer serializer = expectedRegistration.element3;
            if (serializer != null) {
                assertEquals(serializer.getClass().getName(), actualRegistration.element3);
            } else {
                assertNull(actualRegistration.element3);
            }
        }
    }

    private static class TypeClass {
    }

    private static class SerializerClass implements StreamSerializer {
        @Override
        public void write(ObjectDataOutput out, Object object) throws IOException {
        }

        @Override
        public Object read(ObjectDataInput in) throws IOException {
            return null;
        }

        @Override
        public int getTypeId() {
            return 0;
        }

        @Override
        public void destroy() {
        }
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
                .setConsoleEnabled(false)
                .setDataAccessEnabled(true)
                .setTrustedInterfaces(newHashSet("192.168.1.1"));

        Config config = new Config()
                .setManagementCenterConfig(managementCenterConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        ManagementCenterConfig xmlMCConfig = xmlConfig.getManagementCenterConfig();
        assertEquals(managementCenterConfig.isScriptingEnabled(), xmlMCConfig.isScriptingEnabled());
        assertEquals(managementCenterConfig.isConsoleEnabled(), xmlMCConfig.isConsoleEnabled());
        assertEquals(managementCenterConfig.isDataAccessEnabled(), xmlMCConfig.isDataAccessEnabled());
        assertEquals(managementCenterConfig.getTrustedInterfaces(), xmlMCConfig.getTrustedInterfaces());
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
    public void testNativeMemoryWithPersistentMemory() {
        NativeMemoryConfig expectedConfig = new NativeMemoryConfig();
        expectedConfig.setEnabled(true);
        expectedConfig.setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.STANDARD);
        expectedConfig.setMetadataSpacePercentage(12.5f);
        expectedConfig.setMinBlockSize(50);
        expectedConfig.setPageSize(100);
        expectedConfig.setSize(new MemorySize(20, MemoryUnit.MEGABYTES));
        PersistentMemoryConfig origPmemConfig = expectedConfig.getPersistentMemoryConfig();
        origPmemConfig.setEnabled(true);
        origPmemConfig.addDirectoryConfig(new PersistentMemoryDirectoryConfig("/mnt/pmem0", 0));
        origPmemConfig.addDirectoryConfig(new PersistentMemoryDirectoryConfig("/mnt/pmem1", 1));

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

        PersistentMemoryConfig pmemConfig = actualConfig.getPersistentMemoryConfig();
        assertTrue(pmemConfig.isEnabled());
        assertEquals(PersistentMemoryMode.MOUNTED, pmemConfig.getMode());

        List<PersistentMemoryDirectoryConfig> directoryConfigs = pmemConfig.getDirectoryConfigs();
        assertEquals(2, directoryConfigs.size());
        assertEquals("/mnt/pmem0", directoryConfigs.get(0).getDirectory());
        assertEquals(0, directoryConfigs.get(0).getNumaNode());
        assertEquals("/mnt/pmem1", directoryConfigs.get(1).getDirectory());
        assertEquals(1, directoryConfigs.get(1).getNumaNode());
        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testNativeMemoryWithPersistentMemory_SystemMemoryMode() {
        NativeMemoryConfig expectedConfig = new NativeMemoryConfig();
        expectedConfig.setEnabled(true);
        expectedConfig.setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.STANDARD);
        expectedConfig.setMetadataSpacePercentage(12.5f);
        expectedConfig.setMinBlockSize(50);
        expectedConfig.setPageSize(100);
        expectedConfig.setSize(new MemorySize(20, MemoryUnit.MEGABYTES));
        expectedConfig.getPersistentMemoryConfig().setMode(PersistentMemoryMode.SYSTEM_MEMORY);

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

        PersistentMemoryConfig pmemConfig = actualConfig.getPersistentMemoryConfig();
        assertFalse(pmemConfig.isEnabled());
        assertEquals(PersistentMemoryMode.SYSTEM_MEMORY, pmemConfig.getMode());
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
    public void testInstanceTrackingConfig() {
        Config config = new Config();

        config.getInstanceTrackingConfig()
                .setEnabled(true)
                .setFileName("/dummy/file")
                .setFormatPattern("dummy-pattern with $HZ_INSTANCE_TRACKING{placeholder} and $RND{placeholder}");

        InstanceTrackingConfig generatedConfig = getNewConfigViaXMLGenerator(config).getInstanceTrackingConfig();
        assertTrue(generatedConfig + " should be compatible with " + config.getInstanceTrackingConfig(),
                new InstanceTrackingConfigChecker().check(config.getInstanceTrackingConfig(), generatedConfig));
    }

    @Test
    public void testSqlConfig() {
        Config confiig = new Config();

        confiig.getSqlConfig().setStatementTimeoutMillis(30L);

        SqlConfig generatedConfig = getNewConfigViaXMLGenerator(confiig).getSqlConfig();

        assertEquals(confiig.getSqlConfig().getStatementTimeoutMillis(), generatedConfig.getStatementTimeoutMillis());
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
        restApiConfig.enableGroups(RestEndpointGroup.CLUSTER_READ, RestEndpointGroup.HEALTH_CHECK,
                RestEndpointGroup.HOT_RESTART, RestEndpointGroup.WAN);
        restApiConfig.disableGroups(RestEndpointGroup.CLUSTER_WRITE, RestEndpointGroup.DATA);
        Config config = new Config();
        config.getNetworkConfig().setRestApiConfig(restApiConfig);
        RestApiConfig generatedConfig = getNewConfigViaXMLGenerator(config).getNetworkConfig().getRestApiConfig();
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
        RestApiConfig generatedConfig = getNewConfigViaXMLGenerator(config).getNetworkConfig().getRestApiConfig();
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
        RestApiConfig generatedConfig = getNewConfigViaXMLGenerator(config).getNetworkConfig().getRestApiConfig();
        assertTrue(generatedConfig.toString() + " should be compatible with " + restApiConfig.toString(),
                new ConfigCompatibilityChecker.RestApiConfigChecker().check(restApiConfig, generatedConfig));
    }

    @Test
    public void testAdvancedNetworkAutoDetectionJoinConfig() {
        Config cfg = new Config();
        cfg.getAdvancedNetworkConfig().setEnabled(true).getJoin().getAutoDetectionConfig().setEnabled(false);
        Config actualConfig = getNewConfigViaXMLGenerator(cfg);
        assertFalse(actualConfig.getAdvancedNetworkConfig().getJoin().getAutoDetectionConfig().isEnabled());
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
    public void testAuditlogConfig() {
        Config config = new Config();

        config.getAuditlogConfig()
                .setEnabled(true)
                .setFactoryClassName("com.acme.AuditlogToSyslog")
                .setProperty("host", "syslogserver.acme.com")
                .setProperty("port", "514");
        AuditlogConfig generatedConfig = getNewConfigViaXMLGenerator(config).getAuditlogConfig();
        assertTrue(generatedConfig + " should be compatible with " + config.getAuditlogConfig(),
                new ConfigCompatibilityChecker.AuditlogConfigChecker().check(config.getAuditlogConfig(), generatedConfig));
    }

    @Test
    public void testJetConfig() {
        Config config = new Config();
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setEnabled(false).setResourceUploadEnabled(true);
        jetConfig.setLosslessRestartEnabled(true)
                .setScaleUpDelayMillis(123)
                .setBackupCount(2)
                .setFlowControlPeriodMs(123)
                .setCooperativeThreadCount(123);

        jetConfig.getDefaultEdgeConfig()
                .setReceiveWindowMultiplier(123)
                .setPacketSizeLimit(123)
                .setQueueSize(123);

        Config newConfig = getNewConfigViaXMLGenerator(config);
        assertEquals(jetConfig, newConfig.getJetConfig());
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

    @Test
    public void testCacheMerkleTreeConfig() {
        MerkleTreeConfig actual = new MerkleTreeConfig()
                .setEnabled(true)
                .setDepth(22);

        Config cfg = new Config();
        cfg.getCacheConfig("test")
                .setMerkleTreeConfig(actual);

        MerkleTreeConfig expected = getNewConfigViaXMLGenerator(cfg)
                .getCacheConfig("test").getMerkleTreeConfig();

        assertEquals(expected, actual);
    }

    @Test
    public void testCacheWithoutMerkleTreeConfig() {
        Config cfg = new Config();
        MerkleTreeConfig actual = cfg.getCacheConfig("testCacheWithoutMerkleTreeConfig")
                .getMerkleTreeConfig();

        MerkleTreeConfig expected = getNewConfigViaXMLGenerator(cfg)
                .getCacheConfig("testCacheWithoutMerkleTreeConfig").getMerkleTreeConfig();

        assertEquals(expected, actual);
    }

    @Test
    public void testCacheWithDisabledMerkleTreeConfig() {
        MerkleTreeConfig actual = new MerkleTreeConfig()
                .setEnabled(false)
                .setDepth(13);

        Config cfg = new Config();
        cfg.getCacheConfig("testCacheWithDisabledMerkleTreeConfig")
                .setMerkleTreeConfig(actual);

        MerkleTreeConfig expected = getNewConfigViaXMLGenerator(cfg)
                .getCacheConfig("testCacheWithDisabledMerkleTreeConfig").getMerkleTreeConfig();

        assertEquals(expected, actual);
    }

    private Config getNewConfigViaXMLGenerator(Config config) {
        return getNewConfigViaXMLGenerator(config, true);
    }

    private static Config getNewConfigViaXMLGenerator(Config config, boolean maskSensitiveFields) {
        ConfigXmlGenerator configXmlGenerator = new ConfigXmlGenerator(true, maskSensitiveFields);
        String xml = configXmlGenerator.generate(config);
        LOGGER.fine("\n" + xml);
        return new InMemoryXmlConfig(xml);
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
