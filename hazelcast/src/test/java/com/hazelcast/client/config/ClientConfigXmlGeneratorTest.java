/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.config;

import com.hazelcast.client.LoadBalancer;
import com.hazelcast.client.test.CustomLoadBalancer;
import com.hazelcast.client.util.RandomLB;
import com.hazelcast.config.AliasedDiscoveryConfig;
import com.hazelcast.config.CompactSerializationConfig;
import com.hazelcast.config.CompactSerializationConfigAccessor;
import com.hazelcast.config.ConfigCompatibilityChecker;
import com.hazelcast.config.CredentialsFactoryConfig;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.GlobalSerializerConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.InstanceTrackingConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.LoginModuleConfig;
import com.hazelcast.config.LoginModuleConfig.LoginModuleUsage;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.NearCachePreloaderConfig;
import com.hazelcast.config.PersistentMemoryDirectoryConfig;
import com.hazelcast.config.PersistentMemoryMode;
import com.hazelcast.config.PredicateConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.config.security.JaasAuthenticationConfig;
import com.hazelcast.config.security.KerberosIdentityConfig;
import com.hazelcast.config.security.RealmConfig;
import com.hazelcast.config.security.TokenEncoding;
import com.hazelcast.config.security.TokenIdentityConfig;
import com.hazelcast.config.security.UsernamePasswordIdentityConfig;
import com.hazelcast.internal.util.TriTuple;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.memory.Capacity;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.ssl.SSLContextFactory;
import com.hazelcast.spi.eviction.EvictionPolicyComparator;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import example.serialization.EmployeeDTOSerializer;
import example.serialization.EmployerDTO;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.net.ssl.SSLContext;
import java.io.ByteArrayInputStream;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import static com.hazelcast.client.config.ClientConnectionStrategyConfig.ReconnectMode.ASYNC;
import static com.hazelcast.client.config.ClientSqlResubmissionMode.RETRY_SELECTS;
import static com.hazelcast.client.config.impl.ClientAliasedDiscoveryConfigUtils.aliasedDiscoveryConfigsFrom;
import static com.hazelcast.config.EvictionPolicy.LFU;
import static com.hazelcast.config.MaxSizePolicy.USED_NATIVE_MEMORY_SIZE;
import static com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy.CACHE_ON_UPDATE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientConfigXmlGeneratorTest extends HazelcastTestSupport {

    private static final boolean DEBUG = false;
    private static final Random RANDOM = new Random();

    private ClientConfig clientConfig = new ClientConfig();

    @Test
    public void escape() {
        String toEscape = "<>&\"'";
        //escape xml value
        //escape xml attribute
        NearCacheConfig nearCacheConfig = new NearCacheConfig(toEscape);
        clientConfig.setClusterName(toEscape).addNearCacheConfig(nearCacheConfig);

        ClientConfig actual = newConfigViaGenerator();
        assertEquals(clientConfig.getClusterName(), actual.getClusterName());
        assertEquals(toEscape, actual.getNearCacheConfig(toEscape).getName());
    }

    @Test
    public void backupAckToClient() {
        clientConfig.setBackupAckToClientEnabled(false);
        ClientConfig actual = newConfigViaGenerator();
        assertFalse(actual.isBackupAckToClientEnabled());
    }

    @Test
    public void instanceName() {
        String instanceName = randomString();
        clientConfig.setInstanceName(instanceName);
        ClientConfig actual = newConfigViaGenerator();
        assertEquals(instanceName, actual.getInstanceName());
    }

    @Test
    public void labels() {
        clientConfig.addLabel("foo");
        ClientConfig actual = newConfigViaGenerator();
        Set<String> labels = actual.getLabels();
        assertEquals(1, labels.size());
        assertContains(labels, "foo");
    }

    @Test
    public void cluster() {
        String name = randomString();
        clientConfig.setClusterName(name);
        ClientConfig actual = newConfigViaGenerator();
        assertEquals(name, actual.getClusterName());
    }

    @Test
    public void nameAndProperties() {
        String name = randomString();
        String property = randomString();
        clientConfig.setInstanceName(name);
        clientConfig.setProperty("prop", property);
        ClientConfig actual = newConfigViaGenerator();
        assertEquals(name, actual.getInstanceName());
        assertProperties(clientConfig.getProperties(), actual.getProperties());
    }

    @Test
    public void network() {
        ClientNetworkConfig expected = new ClientNetworkConfig();
        expected.setRedoOperation(true)
                .setConnectionTimeout(randomInt())
                .addAddress(randomString())
                .setOutboundPortDefinitions(Collections.singleton(randomString()))
                .getClusterRoutingConfig().setRoutingMode(RoutingMode.MULTI_MEMBER);


        clientConfig.setNetworkConfig(expected);
        ClientNetworkConfig actual = newConfigViaGenerator().getNetworkConfig();

        assertEquals(RoutingMode.MULTI_MEMBER, actual.getClusterRoutingConfig().getRoutingMode());
        assertTrue(actual.isRedoOperation());
        assertEquals(expected.getConnectionTimeout(), actual.getConnectionTimeout());
        assertCollection(expected.getAddresses(), actual.getAddresses());
        assertCollection(expected.getOutboundPortDefinitions(), actual.getOutboundPortDefinitions());
    }

    @Test
    public void networkSocketOptions() {
        SocketOptions expected = new SocketOptions();
        expected.setBufferSize(randomInt())
                .setLingerSeconds(randomInt())
                .setReuseAddress(false)
                .setKeepAlive(false)
                .setTcpNoDelay(false);
        clientConfig.getNetworkConfig().setSocketOptions(expected);

        SocketOptions actual = newConfigViaGenerator().getNetworkConfig().getSocketOptions();
        assertEquals(expected.getBufferSize(), actual.getBufferSize());
        assertEquals(expected.getLingerSeconds(), actual.getLingerSeconds());
        assertEquals(expected.isReuseAddress(), actual.isReuseAddress());
        assertEquals(expected.isKeepAlive(), actual.isKeepAlive());
        assertEquals(expected.isTcpNoDelay(), actual.isTcpNoDelay());
    }

    @Test
    public void networkSocketInterceptor() {
        SocketInterceptorConfig expected = new SocketInterceptorConfig();
        expected.setEnabled(true)
                .setClassName(randomString())
                .setProperty("prop", randomString());
        clientConfig.getNetworkConfig().setSocketInterceptorConfig(expected);

        SocketInterceptorConfig actual = newConfigViaGenerator().getNetworkConfig().getSocketInterceptorConfig();
        assertEquals(expected, actual);
    }

    @Test
    public void networkSsl() {
        SSLConfig expected = new SSLConfig();
        expected.setFactoryClassName(randomString())
                .setEnabled(true)
                .setProperty("prop", randomString());
        clientConfig.getNetworkConfig().setSSLConfig(expected);

        SSLConfig actual = newConfigViaGenerator().getNetworkConfig().getSSLConfig();
        assertEquals(expected, actual);
    }

    @Test
    public void networkSsl_class() {
        SSLConfig expected = new SSLConfig();
        expected.setFactoryImplementation(new TestSSLContextFactory())
            .setEnabled(true)
            .setProperty("prop", randomString());
        clientConfig.getNetworkConfig().setSSLConfig(expected);

        SSLConfig actual = newConfigViaGenerator().getNetworkConfig().getSSLConfig();
        ConfigCompatibilityChecker.checkSSLConfig(expected, actual);
    }

    private static class TestSSLContextFactory implements SSLContextFactory {
        @Override
        public void init(Properties properties) { }

        @Override
        public SSLContext getSSLContext() {
            return null;
        }
    }

    @Test
    public void networkAliasedDiscoveryConfigs() {
        List<AliasedDiscoveryConfig<?>> aliasedDiscoveryConfigs = aliasedDiscoveryConfigsFrom(clientConfig.getNetworkConfig());
        for (AliasedDiscoveryConfig<?> aliasedDiscoveryConfig : aliasedDiscoveryConfigs) {
            aliasedDiscoveryConfig.setUsePublicIp(true).setEnabled(true);
            aliasedDiscoveryConfig.setProperty("testKey", "testValue");
        }
        ClientConfig actualConfig = newConfigViaGenerator();
        List<AliasedDiscoveryConfig<?>> generatedConfigs = aliasedDiscoveryConfigsFrom(actualConfig.getNetworkConfig());

        for (AliasedDiscoveryConfig<?> generatedDiscoveryConfig : generatedConfigs) {
            assertTrue(generatedDiscoveryConfig.isEnabled());
            assertTrue(generatedDiscoveryConfig.isUsePublicIp());
            String testKey = generatedDiscoveryConfig.getProperty("testKey");
            assertEquals("testValue", testKey);
        }
    }

    @Test
    public void networkAutoDetectionConfig() {
        clientConfig.getNetworkConfig().getAutoDetectionConfig().setEnabled(false);
        ClientConfig actualConfig = newConfigViaGenerator();
        assertFalse(actualConfig.getNetworkConfig().getAutoDetectionConfig().isEnabled());
    }

    @Test
    public void networkIcmp() {
        ClientIcmpPingConfig expected = new ClientIcmpPingConfig();
        expected.setEnabled(true)
                .setEchoFailFastOnStartup(false)
                .setIntervalMilliseconds(randomInt())
                .setMaxAttempts(randomInt())
                .setTimeoutMilliseconds(randomInt())
                .setTtl(randomInt());
        clientConfig.getNetworkConfig().setClientIcmpPingConfig(expected);

        ClientIcmpPingConfig actual = newConfigViaGenerator().getNetworkConfig().getClientIcmpPingConfig();
        assertEquals(expected.isEnabled(), actual.isEnabled());
        assertEquals(expected.isEchoFailFastOnStartup(), actual.isEchoFailFastOnStartup());
        assertEquals(expected.getIntervalMilliseconds(), actual.getIntervalMilliseconds());
        assertEquals(expected.getMaxAttempts(), actual.getMaxAttempts());
        assertEquals(expected.getTimeoutMilliseconds(), actual.getTimeoutMilliseconds());
        assertEquals(expected.getTtl(), actual.getTtl());
    }

    @Test
    public void discovery() {
        DiscoveryConfig expected = new DiscoveryConfig();
        expected.setNodeFilterClass(randomString());
        DiscoveryStrategyConfig discoveryStrategy = new DiscoveryStrategyConfig(randomString());
        discoveryStrategy.addProperty("prop", randomString());
        expected.addDiscoveryStrategyConfig(discoveryStrategy);
        clientConfig.getNetworkConfig().setDiscoveryConfig(expected);

        DiscoveryConfig actual = newConfigViaGenerator().getNetworkConfig().getDiscoveryConfig();
        assertEquals(expected.getNodeFilterClass(), actual.getNodeFilterClass());
        assertCollection(expected.getDiscoveryStrategyConfigs(), actual.getDiscoveryStrategyConfigs(),
                new Comparator<>() {
                    @Override
                    public int compare(DiscoveryStrategyConfig o1, DiscoveryStrategyConfig o2) {
                        assertMap(o1.getProperties(), o2.getProperties());
                        return o1.getClassName().equals(o2.getClassName()) ? 0 : -1;
                    }
                });
    }

    @Test
    public void credentialsFactory() {
        Properties props = new Properties();
        props.setProperty("foo", "bar");
        CredentialsFactoryConfig identityConfig = new CredentialsFactoryConfig("com.test.CFactory");
        clientConfig.getSecurityConfig().setCredentialsFactoryConfig(identityConfig);
        ClientConfig actual = newConfigViaGenerator();
        assertEquals(identityConfig, actual.getSecurityConfig().getCredentialsFactoryConfig());
    }

    @Test
    public void usernamePasswordIdentity() {
        UsernamePasswordIdentityConfig identityConfig = new UsernamePasswordIdentityConfig("tester", "s3cr3T");
        clientConfig.getSecurityConfig().setUsernamePasswordIdentityConfig(identityConfig);
        ClientConfig actual = newConfigViaGenerator();
        assertEquals(identityConfig, actual.getSecurityConfig().getUsernamePasswordIdentityConfig());
    }

    @Test
    public void tokenIdentity() {
        TokenIdentityConfig identityConfig = new TokenIdentityConfig(TokenEncoding.BASE64, "bmF6ZGFy");
        clientConfig.getSecurityConfig().setTokenIdentityConfig(identityConfig);
        ClientConfig actual = newConfigViaGenerator();
        assertEquals(identityConfig, actual.getSecurityConfig().getTokenIdentityConfig());
    }

    @Test
    public void kerberosIdentity() {
        KerberosIdentityConfig identityConfig = new KerberosIdentityConfig()
                .setRealm("realm")
                .setSecurityRealm("security-realm")
                .setPrincipal("jduke")
                .setKeytabFile("/opt/keytab")
                .setServiceNamePrefix("prefix")
                .setSpn("spn");
        RealmConfig realmConfig = new RealmConfig().setJaasAuthenticationConfig(new JaasAuthenticationConfig()
                .addLoginModuleConfig(new LoginModuleConfig("test.Krb5LoginModule", LoginModuleUsage.REQUIRED)
                        .setProperty("principal", "jduke")));

        ClientSecurityConfig securityConfig = clientConfig.getSecurityConfig()
            .setKerberosIdentityConfig(identityConfig)
            .addRealmConfig("kerberos", realmConfig);
        ClientConfig actual = newConfigViaGenerator();
        assertEquals(securityConfig, actual.getSecurityConfig());
    }

    @Test
    public void listener() {
        ListenerConfig expected = new ListenerConfig(randomString());
        clientConfig.addListenerConfig(expected);
        ClientConfig actual = newConfigViaGenerator();
        assertCollection(clientConfig.getListenerConfigs(), actual.getListenerConfigs());
    }

    @Test
    public void serialization() {
        SerializationConfig expected = buildSerializationConfig()
            .addSerializerConfig(new SerializerConfig()
                                     .setClassName(TestSerializer.class.getName()).setTypeClassName(TestType.class.getName()));

        clientConfig.setSerializationConfig(expected);
        SerializationConfig actual = newConfigViaGenerator().getSerializationConfig();

        assertEquals(expected.getPortableVersion(), actual.getPortableVersion());
        assertEquals(expected.isUseNativeByteOrder(), actual.isUseNativeByteOrder());
        assertEquals(expected.getByteOrder(), actual.getByteOrder());
        assertEquals(expected.isEnableCompression(), actual.isEnableCompression());
        assertEquals(expected.isEnableSharedObject(), actual.isEnableSharedObject());
        assertEquals(expected.isAllowUnsafe(), actual.isAllowUnsafe());
        assertEquals(expected.isAllowOverrideDefaultSerializers(), actual.isAllowOverrideDefaultSerializers());
        assertEquals(expected.isCheckClassDefErrors(), actual.isCheckClassDefErrors());
        assertEquals(expected.getGlobalSerializerConfig(), actual.getGlobalSerializerConfig());

        assertCollection(expected.getSerializerConfigs(), actual.getSerializerConfigs());
        assertMap(expected.getDataSerializableFactoryClasses(), actual.getDataSerializableFactoryClasses());
        assertMap(expected.getPortableFactoryClasses(), actual.getPortableFactoryClasses());
    }

    @Test
    public void testCompactSerialization() {
        CompactSerializationConfig expected = new CompactSerializationConfig();
        expected.addClass(EmployerDTO.class);
        expected.addSerializer(new EmployeeDTOSerializer());

        clientConfig.getSerializationConfig().setCompactSerializationConfig(expected);

        CompactSerializationConfig actual = newConfigViaGenerator().getSerializationConfig().getCompactSerializationConfig();

        // Since we don't have APIs to register string class names in the
        // compact serialization config, when we read the config from XML/YAML,
        // we store registered classes/serializers in different lists.
        List<String> serializerClassNames
                = CompactSerializationConfigAccessor.getSerializerClassNames(actual);
        List<String> compactSerializableClassNames
                = CompactSerializationConfigAccessor.getCompactSerializableClassNames(actual);

        Map<String, TriTuple<Class, String, CompactSerializer>> registrations
                = CompactSerializationConfigAccessor.getRegistrations(actual);

        for (TriTuple<Class, String, CompactSerializer> registration : registrations.values()) {
            CompactSerializer serializer = registration.element3;
            if (serializer != null) {
                assertThat(serializerClassNames)
                        .contains(serializer.getClass().getName());
            } else {
                assertThat(compactSerializableClassNames)
                        .contains(registration.element1.getName());
            }
        }
    }

    private SerializationConfig buildSerializationConfig() {
        SerializationConfig expected = new SerializationConfig();
        expected.setPortableVersion(randomInt())
            .setUseNativeByteOrder(true)
            .setByteOrder(ByteOrder.LITTLE_ENDIAN)
            .setEnableCompression(true)
            .setEnableSharedObject(false)
            .setAllowUnsafe(true)
            .setAllowOverrideDefaultSerializers(true)
            .setCheckClassDefErrors(false)
            .addDataSerializableFactoryClass(randomInt(), randomString())
            .addPortableFactoryClass(randomInt(), randomString())
            .setGlobalSerializerConfig(new GlobalSerializerConfig()
                                           .setClassName(randomString()).setOverrideJavaSerialization(true));
        return expected;
    }

    @Test
    public void serialization_class() {
        SerializationConfig expected = buildSerializationConfig()
            .addSerializerConfig(new SerializerConfig()
                                     .setClass(TestSerializer.class).setTypeClass(TestType.class));

        clientConfig.setSerializationConfig(expected);
        SerializationConfig actual = newConfigViaGenerator().getSerializationConfig();

        assertEquals(expected.getPortableVersion(), actual.getPortableVersion());
        assertEquals(expected.isUseNativeByteOrder(), actual.isUseNativeByteOrder());
        assertEquals(expected.getByteOrder(), actual.getByteOrder());
        assertEquals(expected.isEnableCompression(), actual.isEnableCompression());
        assertEquals(expected.isEnableSharedObject(), actual.isEnableSharedObject());
        assertEquals(expected.isAllowUnsafe(), actual.isAllowUnsafe());
        assertEquals(expected.isCheckClassDefErrors(), actual.isCheckClassDefErrors());
        assertEquals(expected.getGlobalSerializerConfig(), actual.getGlobalSerializerConfig());

        assertCollection(
            expected.getSerializerConfigs(), actual.getSerializerConfigs(),
            (e, a) -> e.getTypeClass().getName().compareTo(a.getTypeClassName())
        );
        assertMap(expected.getDataSerializableFactoryClasses(), actual.getDataSerializableFactoryClasses());
        assertMap(expected.getPortableFactoryClasses(), actual.getPortableFactoryClasses());
    }

    private static class TestType { }

    private static class TestSerializer implements StreamSerializer {
        @Override
        public void write(ObjectDataOutput out, Object object) {

        }

        @Override
        public Object read(ObjectDataInput in) {
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
    public void nativeMemory() {
        NativeMemoryConfig expected = new NativeMemoryConfig();
        expected.setEnabled(true)
                .setAllocatorType(MemoryAllocatorType.STANDARD)
                .setMetadataSpacePercentage(70)
                .setMinBlockSize(randomInt())
                .setPageSize(randomInt())
                .setCapacity(new Capacity(randomInt(), MemoryUnit.BYTES));
        clientConfig.setNativeMemoryConfig(expected);

        NativeMemoryConfig actual = newConfigViaGenerator().getNativeMemoryConfig();
        assertEquals(clientConfig.getNativeMemoryConfig(), actual);
    }

    @Test
    public void nativeMemoryWithPersistentMemory() {
        NativeMemoryConfig expected = new NativeMemoryConfig();
        expected.setEnabled(true)
                .setAllocatorType(MemoryAllocatorType.STANDARD)
                .setMetadataSpacePercentage(70)
                .setMinBlockSize(randomInt())
                .setPageSize(randomInt())
                .setCapacity(new Capacity(randomInt(), MemoryUnit.BYTES))
                .getPersistentMemoryConfig()
                .setEnabled(true)
                .addDirectoryConfig(new PersistentMemoryDirectoryConfig("/mnt/pmem0", 0))
                .addDirectoryConfig(new PersistentMemoryDirectoryConfig("/mnt/pmem1", 1));
        clientConfig.setNativeMemoryConfig(expected);

        NativeMemoryConfig actual = newConfigViaGenerator().getNativeMemoryConfig();
        assertEquals(clientConfig.getNativeMemoryConfig(), actual);
    }

    @Test
    public void nativeMemoryWithPersistentMemory_SystemMemoryMode() {
        NativeMemoryConfig expected = new NativeMemoryConfig();
        expected.setEnabled(true)
                .setAllocatorType(MemoryAllocatorType.STANDARD)
                .setMetadataSpacePercentage(70)
                .setMinBlockSize(randomInt())
                .setPageSize(randomInt())
                .setCapacity(new Capacity(randomInt(), MemoryUnit.BYTES))
                .getPersistentMemoryConfig()
                .setMode(PersistentMemoryMode.SYSTEM_MEMORY);
        clientConfig.setNativeMemoryConfig(expected);

        NativeMemoryConfig actual = newConfigViaGenerator().getNativeMemoryConfig();
        assertEquals(clientConfig.getNativeMemoryConfig(), actual);
    }

    @Test
    public void proxyFactory() {
        ProxyFactoryConfig expected = new ProxyFactoryConfig();
        expected.setClassName(randomString())
                .setService(randomString());
        clientConfig.addProxyFactoryConfig(expected);

        List<ProxyFactoryConfig> actual = newConfigViaGenerator().getProxyFactoryConfigs();
        assertCollection(clientConfig.getProxyFactoryConfigs(), actual);
    }

    @Test
    public void loadBalancer() {
        clientConfig.setLoadBalancer(new RandomLB());
        ClientConfig newClientConfig = newConfigViaGenerator();
        LoadBalancer actual = newClientConfig.getLoadBalancer();
        assertTrue(actual instanceof RandomLB);
        String actualClassName = newClientConfig.getLoadBalancerClassName();
        assertNull(actualClassName);
    }

    @Test
    public void loadBalancerCustom() {
        clientConfig.setLoadBalancer(new CustomLoadBalancer());
        ClientConfig newClientConfig = newConfigViaGenerator();
        LoadBalancer actual = newClientConfig.getLoadBalancer();
        assertNull(actual);
        String actualClassName = newClientConfig.getLoadBalancerClassName();
        assertEquals("com.hazelcast.client.test.CustomLoadBalancer", actualClassName);
    }

    private NearCacheConfig createNearCacheConfig(String name) {
        NearCacheConfig expected = new NearCacheConfig();
        expected.setInMemoryFormat(InMemoryFormat.NATIVE)
            .setSerializeKeys(true)
            .setInvalidateOnChange(false)
            .setTimeToLiveSeconds(randomInt())
            .setMaxIdleSeconds(randomInt())
            .setLocalUpdatePolicy(CACHE_ON_UPDATE)
            .setName(name);
        return expected;
    }

    @Test
    public void nearCache() {
        NearCacheConfig expected = createNearCacheConfig(randomString())
                .setPreloaderConfig(
                        new NearCachePreloaderConfig()
                                .setEnabled(true)
                                .setDirectory(randomString())
                                .setStoreInitialDelaySeconds(randomInt())
                                .setStoreIntervalSeconds(randomInt())
                )
                .setEvictionConfig(
                        new EvictionConfig()
                                .setEvictionPolicy(LFU)
                                .setMaxSizePolicy(USED_NATIVE_MEMORY_SIZE)
                                .setComparatorClassName(randomString())
                                .setSize(randomInt())
                );
        clientConfig.addNearCacheConfig(expected);

        Map<String, NearCacheConfig> actual = newConfigViaGenerator().getNearCacheConfigMap();
        assertMap(clientConfig.getNearCacheConfigMap(), actual);
    }

    @Test
    public void nearCache_evictionConfigWithPolicyComparator() {
        String nearCacheName = randomString();
        NearCacheConfig expected = createNearCacheConfig(nearCacheName)
            .setPreloaderConfig(
                new NearCachePreloaderConfig()
                    .setEnabled(true)
                    .setDirectory(randomString())
                    .setStoreInitialDelaySeconds(randomInt())
                    .setStoreIntervalSeconds(randomInt())
            )
            .setEvictionConfig(
                new EvictionConfig()
                    .setEvictionPolicy(LFU)
                    .setMaxSizePolicy(USED_NATIVE_MEMORY_SIZE)
                    .setComparator(new TestEvictionPolicyComparator())
                    .setSize(randomInt())
            );
        clientConfig.addNearCacheConfig(expected);

        NearCacheConfig actual = newConfigViaGenerator().getNearCacheConfigMap().get(nearCacheName);
        ConfigCompatibilityChecker.checkNearCacheConfig(expected, actual);
    }

    private static class TestEvictionPolicyComparator implements EvictionPolicyComparator {
        @Override
        public int compare(Object o1, Object o2) {
            return 0;
        }
    }

    private QueryCacheConfig createQueryCacheConfig(String name) {
        return new QueryCacheConfig()
            .setBufferSize(randomInt())
            .setInMemoryFormat(InMemoryFormat.OBJECT)
            .setName(name)
            .setBatchSize(randomInt())
            .setCoalesce(true)
            .setDelaySeconds(randomInt())
            .setIncludeValue(false)
            .setPopulate(false)
            .setPredicateConfig(new PredicateConfig(randomString()))
            .setEvictionConfig(
                new EvictionConfig()
                    .setEvictionPolicy(LFU)
                    .setMaxSizePolicy(USED_NATIVE_MEMORY_SIZE)
                    .setComparatorClassName(randomString())
                    .setSize(randomInt())
            ).addIndexConfig(
                new IndexConfig()
                    .setType(IndexType.SORTED)
                    .addAttribute(randomString())
            );
    }

    @Test
    public void queryCache() {
        QueryCacheConfig expected = createQueryCacheConfig(randomString())
            .addEntryListenerConfig(
                (EntryListenerConfig) new EntryListenerConfig()
                    .setIncludeValue(true)
                    .setLocal(true)
                    .setClassName(randomString()));
        clientConfig.addQueryCacheConfig(randomString(), expected);

        Map<String, Map<String, QueryCacheConfig>> actual = newConfigViaGenerator().getQueryCacheConfigs();
        assertMap(clientConfig.getQueryCacheConfigs(), actual);
    }

    @Test
    public void queryCache_withEntryEventListenerClass() {
        String queryCacheName = randomString();
        QueryCacheConfig expected = createQueryCacheConfig(queryCacheName)
            .addEntryListenerConfig(
                new EntryListenerConfig()
                    .setIncludeValue(true)
                    .setLocal(true)
                    .setImplementation(new TestMapListener()));
        String mapName = randomString();
        clientConfig.addQueryCacheConfig(mapName, expected);

        QueryCacheConfig actual = newConfigViaGenerator().getQueryCacheConfigs().get(mapName).get(queryCacheName);
        ConfigCompatibilityChecker.checkQueryCacheConfig(expected, actual);
    }

    private static class TestMapListener implements MapListener { }

    @Test
    public void connectionStrategy() {
        ClientConnectionStrategyConfig expected = new ClientConnectionStrategyConfig();
        expected.setAsyncStart(true)
                .setReconnectMode(ASYNC)
                .setConnectionRetryConfig(new ConnectionRetryConfig()
                        .setInitialBackoffMillis(1000)
                        .setMaxBackoffMillis(30000)
                        .setMultiplier(2.0)
                        .setClusterConnectTimeoutMillis(5)
                        .setJitter(0.2));
        clientConfig.setConnectionStrategyConfig(expected);

        ClientConnectionStrategyConfig actual = newConfigViaGenerator().getConnectionStrategyConfig();
        assertEquals(expected.isAsyncStart(), actual.isAsyncStart());
        assertEquals(expected.getReconnectMode(), actual.getReconnectMode());
        assertEquals(expected.getConnectionRetryConfig(), actual.getConnectionRetryConfig());
    }

    @Test
    public void userCodeDeployment() {
        ClientUserCodeDeploymentConfig expected = new ClientUserCodeDeploymentConfig();
        expected.setEnabled(true)
                .addClass(randomString())
                .addJar(randomString());
        clientConfig.setUserCodeDeploymentConfig(expected);

        ClientUserCodeDeploymentConfig actual = newConfigViaGenerator().getUserCodeDeploymentConfig();
        assertEquals(expected.isEnabled(), actual.isEnabled());
        assertCollection(expected.getClassNames(), actual.getClassNames());
        assertCollection(expected.getJarPaths(), actual.getJarPaths());
    }

    @Test
    public void flakeIdGenerator() {
        ClientFlakeIdGeneratorConfig expected = new ClientFlakeIdGeneratorConfig(randomString());
        expected.setPrefetchCount(randomInt())
                .setPrefetchValidityMillis(randomInt());
        clientConfig.addFlakeIdGeneratorConfig(expected);

        Map<String, ClientFlakeIdGeneratorConfig> actual = newConfigViaGenerator().getFlakeIdGeneratorConfigMap();
        assertMap(clientConfig.getFlakeIdGeneratorConfigMap(), actual);
    }

    @Test
    public void testMetricsConfig() {
        clientConfig.getMetricsConfig()
                    .setEnabled(false)
                    .setCollectionFrequencySeconds(10);

        clientConfig.getMetricsConfig().getJmxConfig()
                    .setEnabled(false);

        ClientMetricsConfig originalConfig = clientConfig.getMetricsConfig();
        ClientMetricsConfig generatedConfig = newConfigViaGenerator().getMetricsConfig();
        assertEquals(originalConfig.isEnabled(), generatedConfig.isEnabled());
        assertEquals(originalConfig.getJmxConfig().isEnabled(), generatedConfig.getJmxConfig().isEnabled());
        assertEquals(originalConfig.getCollectionFrequencySeconds(), generatedConfig.getCollectionFrequencySeconds());
    }

    @Test
    public void testInstanceTrackingConfig() {
        clientConfig.getInstanceTrackingConfig()
                    .setEnabled(true)
                    .setFileName("/dummy/file")
                    .setFormatPattern("dummy-pattern with $HZ_INSTANCE_TRACKING{placeholder} and $RND{placeholder}");

        InstanceTrackingConfig originalConfig = clientConfig.getInstanceTrackingConfig();
        InstanceTrackingConfig generatedConfig = newConfigViaGenerator().getInstanceTrackingConfig();
        assertEquals(originalConfig.isEnabled(), generatedConfig.isEnabled());
        assertEquals(originalConfig.getFileName(), generatedConfig.getFileName());
        assertEquals(originalConfig.getFormatPattern(), generatedConfig.getFormatPattern());
    }

    @Test
    public void testSqlConfig() {
        ClientSqlConfig originalConfig = new ClientSqlConfig().setResubmissionMode(RETRY_SELECTS);
        clientConfig.setSqlConfig(originalConfig);
        ClientSqlConfig generatedConfig = newConfigViaGenerator().getSqlConfig();
        assertEquals(originalConfig.getResubmissionMode(), generatedConfig.getResubmissionMode());
    }

    @Test
    public void testTpcConfig() {
        ClientTpcConfig originalConfig = new ClientTpcConfig()
                .setEnabled(true)
                .setConnectionCount(10);
        clientConfig.setTpcConfig(originalConfig);
        ClientTpcConfig generatedConfig = newConfigViaGenerator().getTpcConfig();
        assertEquals(originalConfig, generatedConfig);
    }

    @Test
    public void testNetworkCloud() {
        ClientCloudConfig originalConfig = new ClientCloudConfig().setEnabled(true)
                .setDiscoveryToken("pAB2kwCdHKbGpFBNd9iO9AmnYBiQa7rz8yfGW25iHEHRvoRWSN");
        clientConfig.getNetworkConfig().setCloudConfig(originalConfig);
        ClientCloudConfig generatedConfig = newConfigViaGenerator().getNetworkConfig().getCloudConfig();
        assertEquals(originalConfig, generatedConfig);
    }

    private ClientConfig newConfigViaGenerator() {
        String xml = ClientConfigXmlGenerator.generate(clientConfig, DEBUG ? 5 : -1);
        debug(xml);
        return new XmlClientConfigBuilder(new ByteArrayInputStream(xml.getBytes())).build();
    }

    private static int randomInt() {
        return randomInt(1000);
    }

    private static int randomInt(int bound) {
        return RANDOM.nextInt(bound) + 1;
    }

    private static <K, V> void assertMap(Map<K, V> expected, Map<K, V> actual) {
        assertEquals(expected.size(), actual.size());
        assertContainsAll(actual.entrySet(), expected.entrySet());
    }

    private static void assertProperties(Properties expected, Properties actual) {
        assertEquals(expected.size(), actual.size());
        Set<String> names = expected.stringPropertyNames();
        for (String name : names) {
            assertEquals(expected.getProperty(name), actual.getProperty(name));
        }
    }

    private static void debug(String s) {
        if (DEBUG) {
            System.out.println(s);
        }
    }

}
