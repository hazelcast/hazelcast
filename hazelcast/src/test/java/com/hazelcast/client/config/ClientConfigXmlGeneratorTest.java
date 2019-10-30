/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.util.RandomLB;
import com.hazelcast.config.AliasedDiscoveryConfig;
import com.hazelcast.config.CredentialsFactoryConfig;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.GlobalSerializerConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.NearCachePreloaderConfig;
import com.hazelcast.config.PredicateConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.config.security.TokenEncoding;
import com.hazelcast.config.security.TokenIdentityConfig;
import com.hazelcast.config.security.UsernamePasswordIdentityConfig;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.nio.ByteOrder;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import static com.hazelcast.client.config.impl.ClientAliasedDiscoveryConfigUtils.aliasedDiscoveryConfigsFrom;
import static com.hazelcast.client.config.ClientConnectionStrategyConfig.ReconnectMode.ASYNC;
import static com.hazelcast.config.MaxSizePolicy.USED_NATIVE_MEMORY_SIZE;
import static com.hazelcast.config.EvictionPolicy.LFU;
import static com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy.CACHE_ON_UPDATE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
        expected.setSmartRouting(false)
                .setRedoOperation(true)
                .setConnectionTimeout(randomInt())
                .addAddress(randomString())
                .setOutboundPortDefinitions(Collections.singleton(randomString()));


        clientConfig.setNetworkConfig(expected);
        ClientNetworkConfig actual = newConfigViaGenerator().getNetworkConfig();

        assertFalse(actual.isSmartRouting());
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
        assertEquals(expected.isEnabled(), actual.isEnabled());
        assertEquals(expected.getFactoryClassName(), actual.getFactoryClassName());
        assertProperties(expected.getProperties(), actual.getProperties());
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
                new Comparator<DiscoveryStrategyConfig>() {
                    @Override
                    public int compare(DiscoveryStrategyConfig o1, DiscoveryStrategyConfig o2) {
                        assertMap(o1.getProperties(), o2.getProperties());
                        return o1.getClassName().equals(o2.getClassName()) ? 0 : -1;
                    }
                });
    }

    @Test
    public void executorPoolSize() {
        clientConfig.setExecutorPoolSize(randomInt());
        ClientConfig actual = newConfigViaGenerator();
        assertEquals(clientConfig.getExecutorPoolSize(), actual.getExecutorPoolSize());
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
    public void listener() {
        ListenerConfig expected = new ListenerConfig(randomString());
        clientConfig.addListenerConfig(expected);
        ClientConfig actual = newConfigViaGenerator();
        assertCollection(clientConfig.getListenerConfigs(), actual.getListenerConfigs());
    }

    @Test
    public void serialization() {
        SerializationConfig expected = new SerializationConfig();
        expected.setPortableVersion(randomInt())
                .setUseNativeByteOrder(true)
                .setByteOrder(ByteOrder.LITTLE_ENDIAN)
                .setEnableCompression(true)
                .setEnableSharedObject(false)
                .setAllowUnsafe(true)
                .setCheckClassDefErrors(false)
                .addDataSerializableFactoryClass(randomInt(), randomString())
                .addPortableFactoryClass(randomInt(), randomString())
                .setGlobalSerializerConfig(new GlobalSerializerConfig()
                        .setClassName(randomString()).setOverrideJavaSerialization(true))
                .addSerializerConfig(new SerializerConfig()
                        .setClassName(randomString()).setTypeClassName(randomString()));

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

        assertCollection(expected.getSerializerConfigs(), actual.getSerializerConfigs());
        assertMap(expected.getDataSerializableFactoryClasses(), actual.getDataSerializableFactoryClasses());
        assertMap(expected.getPortableFactoryClasses(), actual.getPortableFactoryClasses());
    }

    @Test
    public void nativeMemory() {
        NativeMemoryConfig expected = new NativeMemoryConfig();
        expected.setEnabled(true)
                .setAllocatorType(MemoryAllocatorType.STANDARD)
                .setMetadataSpacePercentage(70)
                .setMinBlockSize(randomInt())
                .setPageSize(randomInt())
                .setSize(new MemorySize(randomInt(), MemoryUnit.BYTES));
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
        LoadBalancer actual = newConfigViaGenerator().getLoadBalancer();
        assertTrue(actual instanceof RandomLB);
    }

    @Test
    public void nearCache() {
        NearCacheConfig expected = new NearCacheConfig();
        expected.setInMemoryFormat(InMemoryFormat.NATIVE)
                .setSerializeKeys(true)
                .setInvalidateOnChange(false)
                .setTimeToLiveSeconds(randomInt())
                .setMaxIdleSeconds(randomInt())
                .setLocalUpdatePolicy(CACHE_ON_UPDATE)
                .setName(randomString())
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
                                //Comparator class name cannot set via xml
                                //see https://github.com/hazelcast/hazelcast/issues/14093
                                //.setComparatorClassName(randomString())
                                .setSize(randomInt())
                );
        clientConfig.addNearCacheConfig(expected);

        Map<String, NearCacheConfig> actual = newConfigViaGenerator().getNearCacheConfigMap();
        assertMap(clientConfig.getNearCacheConfigMap(), actual);
    }

    @Test
    public void queryCache() {
        QueryCacheConfig expected = new QueryCacheConfig();
        expected.setBufferSize(randomInt())
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setName(randomString())
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
                                //Comparator class name cannot set via xml
                                //see https://github.com/hazelcast/hazelcast/issues/14093
                                //.setComparatorClassName(randomString())
                                .setSize(randomInt())
                ).addIndexConfig(
                new IndexConfig()
                        .setType(IndexType.SORTED)
                        .addAttribute(randomString())
        ).addEntryListenerConfig(
                (EntryListenerConfig) new EntryListenerConfig()
                        .setIncludeValue(true)
                        .setLocal(true)
                        .setClassName(randomString()));
        clientConfig.addQueryCacheConfig(randomString(), expected);

        Map<String, Map<String, QueryCacheConfig>> actual = newConfigViaGenerator().getQueryCacheConfigs();
        assertMap(clientConfig.getQueryCacheConfigs(), actual);
    }

    @Test
    public void connectionStrategy() {
        ClientConnectionStrategyConfig expected = new ClientConnectionStrategyConfig();
        expected.setAsyncStart(true)
                .setReconnectMode(ASYNC)
                .setConnectionRetryConfig(new ConnectionRetryConfig()
                        .setInitialBackoffMillis(1000)
                        .setMaxBackoffMillis(30000)
                        .setMultiplier(2.0)
                        .setFailOnMaxBackoff(true)
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

    private static <T> void assertCollection(Collection<T> expected, Collection<T> actual) {
        assertEquals(expected.size(), actual.size());
        assertContainsAll(actual, expected);
    }

    private static <T> void assertCollection(Collection<T> expected, Collection<T> actual, Comparator<T> comparator) {
        assertEquals(expected.size(), actual.size());
        for (T item : expected) {
            if (!contains(item, actual, comparator)) {
                throw new AssertionError("Actual collection does not contain the item " + item);
            }
        }
    }

    private static <T> boolean contains(T item1, Collection<T> collection, Comparator<T> comparator) {
        for (T item2 : collection) {
            if (comparator.compare(item1, item2) == 0) {
                return true;
            }
        }
        return false;
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
