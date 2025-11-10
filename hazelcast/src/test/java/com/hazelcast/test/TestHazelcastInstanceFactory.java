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

package com.hazelcast.test;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.PersistenceClusterDataRecoveryPolicy;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.config.YamlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.instance.AddressPicker;
import com.hazelcast.instance.impl.DefaultNodeContext;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeContext;
import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.internal.cluster.Joiner;
import com.hazelcast.internal.metrics.MetricsPublisher;
import com.hazelcast.internal.metrics.impl.MetricsService;
import com.hazelcast.internal.server.FirewallingServer;
import com.hazelcast.internal.server.Server;
import com.hazelcast.internal.server.tcp.LocalAddressRegistry;
import com.hazelcast.internal.server.tcp.ServerSocketRegistry;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.metrics.MetricsRule;
import com.hazelcast.test.mocknetwork.TestNodeRegistry;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.IntStream;

import static com.hazelcast.internal.config.DeclarativeConfigUtil.SYSPROP_MEMBER_CONFIG;
import static com.hazelcast.internal.config.DeclarativeConfigUtil.YAML_ACCEPTED_SUFFIXES;
import static com.hazelcast.internal.config.DeclarativeConfigUtil.isAcceptedSuffixConfigured;
import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.test.Accessors.getAddress;
import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.HazelcastTestSupport.assertClusterSizeEventually;
import static com.hazelcast.test.HazelcastTestSupport.spawn;
import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableCollection;
import static java.util.Comparator.comparing;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

@ThreadSafe
public class TestHazelcastInstanceFactory {
    private static final int DEFAULT_INITIAL_PORT = NetworkConfig.DEFAULT_PORT;
    private static final int MAX_PORT_NUMBER = (1 << 16) - 1;

    protected final TestNodeRegistry registry;

    protected final boolean isMockNetwork = TestEnvironment.isMockNetwork();
    private final ConcurrentMap<Integer, Address> addressMap = new ConcurrentHashMap<>();
    private final AtomicInteger nodeIndex = new AtomicInteger();
    private final int count;

    private volatile UnaryOperator<NodeContext> nodeContextDelegator = UnaryOperator.identity();
    private volatile MetricsRule metricsRule;

    public TestHazelcastInstanceFactory() {
        this(0);
    }

    public TestHazelcastInstanceFactory(int initialPort, String... addresses) {
        fillAddressMap(initialPort, addresses);
        count = addresses.length;
        registry = isMockNetwork ? createRegistry() : null;
    }

    public TestHazelcastInstanceFactory(String... addresses) {
        this(-1, addresses);
    }

    public TestHazelcastInstanceFactory(int count) {
        fillAddressMap(count);
        this.count = count;
        registry = isMockNetwork ? createRegistry() : null;
    }

    /** Cannot be used in combination with {@link #withNodeExtensionCustomizer}. */
    public TestHazelcastInstanceFactory withNodeContextDelegator(UnaryOperator<NodeContext> nodeContextDelegator) {
        this.nodeContextDelegator = nodeContextDelegator;
        return this;
    }

    /** Cannot be used in combination with {@link #withNodeContextDelegator}. */
    public TestHazelcastInstanceFactory withNodeExtensionCustomizer(Function<Node, NodeExtension> nodeExtensionFn) {
        nodeContextDelegator = nodeContext -> new NodeExtensionCustomizer(nodeContext, nodeExtensionFn);
        return this;
    }

    public TestHazelcastInstanceFactory withMetricsRule(MetricsRule metricsRule) {
        this.metricsRule = metricsRule;
        return this;
    }

    private TestNodeRegistry createRegistry() {
        return new TestNodeRegistry(getKnownAddresses());
    }

    @Nullable
    public TestNodeRegistry getRegistry() {
        return registry;
    }

    public int getCount() {
        return count;
    }

    /**
     * Equivalent to {@link #newHazelcastInstance(Config) newHazelcastInstance(null)}.
     */
    public HazelcastInstance newHazelcastInstance() {
        return newHazelcastInstance((Config) null);
    }

    /**
     * Creates a new test Hazelcast instance.
     *
     * @param config if {@code null}, the default config is used.
     */
    public HazelcastInstance newHazelcastInstance(Config config) {
        return newHazelcastInstance(null, null, config, emptySet());
    }

    /**
     * Equivalent to {@link #newHazelcastInstance(Address, Config) newHazelcastInstance(address, null)}.
     */
    public HazelcastInstance newHazelcastInstance(Address address) {
        return newHazelcastInstance(address, null);
    }

    /**
     * Creates a new test Hazelcast instance.
     *
     * @param address if {@code null}, {@linkplain #nextAddress() the next address} is used.
     * @param config  if {@code null}, the default config is used.
     */
    public HazelcastInstance newHazelcastInstance(Address address, Config config) {
        return newHazelcastInstance(address, null, config, emptySet());
    }

    protected HazelcastInstance newHazelcastInstance(Address address, UUID uuid, Config config,
                                                     @Nonnull Set<Address> blockedAddresses) {
        NodeContext nodeContext;
        if (isMockNetwork) {
            config = initOrCreateConfig(config);
            if (address == null) {
                address = nextAddress(config.getNetworkConfig().getPort());
            }
            nodeContext = registry.createNodeContext(address, blockedAddresses);
        } else {
            if (address != null || !blockedAddresses.isEmpty()) {
                throw new UnsupportedOperationException((address != null ? "Explicit" : "Blocked")
                        + " addresses are only available in mock network");
            }
            nodeContext = new DefaultNodeContext();
        }
        nodeContext = withUUID(nodeContextDelegator.apply(nodeContext), uuid);
        String instanceName = config != null ? config.getInstanceName() : null;
        HazelcastInstance instance =
                HazelcastInstanceFactory.newHazelcastInstance(config, instanceName, nodeContext);
        registerTestMetricsPublisher(instance);
        return instance;
    }

    private static NodeContext withUUID(NodeContext nodeContext, @Nullable UUID uuid) {
        if (uuid != null) {
            nodeContext = spy(nodeContext);
            doAnswer(invocation -> {
                NodeExtension nodeExtension = spy((NodeExtension) invocation.callRealMethod());
                doReturn(uuid).when(nodeExtension).createMemberUuid();
                return nodeExtension;
            }).when(nodeContext).createNodeExtension(any());
        }
        return nodeContext;
    }

    public HazelcastInstanceBuilder builder() {
        return new HazelcastInstanceBuilder();
    }

    public class HazelcastInstanceBuilder {
        private Address address;
        private UUID uuid;
        private Config config;
        private Set<Address> blockedAddresses = emptySet();

        /** If skipped, or passed {@code null}, {@linkplain #nextAddress() the next address} is used. */
        public HazelcastInstanceBuilder withAddress(Address address) {
            this.address = address;
            return this;
        }

        /**
         * If skipped, or passed {@code null}: <ol>
         * <li> if persistence is enabled and the member acquires an existing hot restart directory,
         *      the local member UUID is read from {@code cluster/members.bin};
         * <li> otherwise, a new securely random UUID is created.
         *
         * @see com.hazelcast.instance.impl.EnterpriseNodeExtension#createMemberUuid()
         */
        public HazelcastInstanceBuilder withUuid(UUID uuid) {
            this.uuid = uuid;
            return this;
        }

        /** If skipped, or passed {@code null}, the default config is used. */
        public HazelcastInstanceBuilder withConfig(Config config) {
            this.config = config;
            return this;
        }

        /**
         * {@code blockedAddresses} are blacklisted in {@code MockJoiner}
         * and connections to them are blocked by {@code FirewallingServer}.
         * <p>
         * This is handy in split-brain tests, when a new instance should be
         * started on a specific network partition of the split brain.
         */
        public HazelcastInstanceBuilder withBlockedAddresses(@Nonnull Set<Address> blockedAddresses) {
            this.blockedAddresses = blockedAddresses;
            return this;
        }

        public HazelcastInstance construct() {
            return newHazelcastInstance(address, uuid, config, blockedAddresses);
        }
    }

    private void registerTestMetricsPublisher(HazelcastInstance instance) {
        if (metricsRule != null && metricsRule.isEnabled()) {
            MetricsService metricService = getNodeEngineImpl(instance).getService(MetricsService.SERVICE_NAME);
            metricService.registerPublisher(
                    (FunctionEx<NodeEngine, MetricsPublisher>) nodeEngine -> metricsRule.getMetricsPublisher(instance));
        }
    }

    public Address nextAddress() {
        return nextAddress(DEFAULT_INITIAL_PORT);
    }

    private Address nextAddress(int initialPort) {
        if (isMockNetwork) {
            int id = nodeIndex.getAndIncrement();
            Address currentAddress = addressMap.get(id);
            if (currentAddress != null) {
                return currentAddress;
            }
            return addNewAddressToAddressMap(id, "127.0.0.1", initialPort);
        }
        throw new UnsupportedOperationException("Explicit addresses are only available in mock network");
    }

    public HazelcastInstance[] newInstances() {
        return newInstances(new Config());
    }

    public HazelcastInstance[] newInstances(Config config, int nodeCount) {
        return newInstances(() -> config, nodeCount);
    }

    public HazelcastInstance[] newInstances(Supplier<Config> configSupplier, int nodeCount) {
        return newInstances(nodeCount, i -> configSupplier.get());
    }

    public HazelcastInstance[] newInstances(int nodeCount, IntFunction<Config> configFn) {
        HazelcastInstance[] instances = new HazelcastInstance[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            instances[i] = newHazelcastInstance(configFn.apply(i));
        }
        return instances;
    }

    public HazelcastInstance[] newInstances(Config config) {
        return newInstances(config, count);
    }

    public HazelcastInstancesBuilder newInstancesParallel() {
        return new HazelcastInstancesBuilder();
    }

    public class HazelcastInstancesBuilder {
        private int nodeCount;
        private IntFunction<Address> addressFn = i -> null;
        private BiFunction<Integer, Address, Config> configFn = (i, address) -> null;
        private boolean ignoreErrors;
        private boolean matchAgainstNodeCount;

        public HazelcastInstancesBuilder withNodeCount(int nodeCount) {
            this.nodeCount = nodeCount;
            return this;
        }

        /**
         * Uses {@linkplain #nextAddress() the next address} for created instances.
         * <p>
         * This can only be used with mock network since it uses explicit addresses.
         */
        public HazelcastInstancesBuilder withNextAddress() {
            addressFn = i -> nextAddress();
            return this;
        }

        /**
         * Uses the specified addresses for created instances. It also sets the
         * {@linkplain #withNodeCount node count}.
         * <p>
         * This can only be used with mock network since it uses explicit addresses.
         */
        public HazelcastInstancesBuilder withAddresses(Address[] addresses) {
            nodeCount = addresses.length;
            addressFn = i -> addresses[i];
            return this;
        }

        /**
         * @param configFn an index-accepting function that must return a separate
         *                 config instance for each index
         */
        public HazelcastInstancesBuilder withConfigFn(IntFunction<Config> configFn) {
            this.configFn = (i, address) -> configFn.apply(i);
            return this;
        }

        /**
         * @param configFn an address-accepting function that must return a separate
         *                 config instance for each address
         */
        public HazelcastInstancesBuilder withConfigFn(Function<Address, Config> configFn) {
            this.configFn = (i, address) -> configFn.apply(address);
            return this;
        }

        /**
         * Ignores failed instances, which is useful if partial recovery is allowed.
         *
         * @see PersistenceClusterDataRecoveryPolicy
         */
        public HazelcastInstancesBuilder ignoreErrors() {
            ignoreErrors = true;
            return this;
        }

        /**
         * Verifies the cluster size by matching against the expected node count, as opposed
         * to matching against the HazelcastInstance[] array size after initial startup. This
         * is useful if nodes which fail startup are expected to restart again immediately.
         */
        public HazelcastInstancesBuilder matchAgainstNodeCount() {
            matchAgainstNodeCount = true;
            return this;
        }

        /**
         * Creates the given number of Hazelcast instances in parallel.
         * The first member in the returned array is always the master.
         * <p>
         * Spawns a separate thread to start each instance. This is required when
         * starting a Hot Restart-enabled cluster, where the {@code newHazelcastInstance()}
         * call blocks until the whole cluster is re-formed.
         */
        public HazelcastInstance[] construct() {
            List<Throwable> errors = new ArrayList<>();
            HazelcastInstance[] instances = IntStream.range(0, nodeCount)
                     .mapToObj(i -> spawn(() -> {
                         Address address = addressFn.apply(i);
                         return newHazelcastInstance(address, configFn.apply(i, address));
                     }))
                     // We need to collect here to ensure that all threads are
                     // spawned before we call future.get()
                     .toList().stream()
                     .map(future -> {
                         try {
                             return future.get();
                         } catch (Throwable t) {
                             errors.add(t);
                             return null;
                         }
                     })
                     .filter(Objects::nonNull)
                     .toArray(HazelcastInstance[]::new);

            if (!ignoreErrors && !errors.isEmpty()) {
                Throwable error = errors.remove(0);
                errors.forEach(error::addSuppressed);
                throw sneakyThrow(error);
            }
            if (matchAgainstNodeCount) {
                assertClusterSizeEventually(nodeCount, instances);
            } else {
                assertClusterSizeEventually(instances.length, instances);
            }
            Arrays.sort(instances, comparing(hz -> !getNode(hz).isMaster()));
            return instances;
        }
    }

    /**
     * Returns all running Hazelcast instances. In mock network, the instances
     * that are not yet started are included. In real network, the method is
     * blocked until all instances are started.
     */
    public Collection<HazelcastInstance> getAllHazelcastInstances() {
        if (isMockNetwork) {
            return registry.getAllHazelcastInstances();
        }
        return Hazelcast.getAllHazelcastInstances();
    }

    /**
     * Returns all known addresses. In mock network: <ol>
     * <li> The addresses corresponding to instances that are not yet created, which
     *      can be specified via {@link #TestHazelcastInstanceFactory(int, String...)},
     *      {@link #TestHazelcastInstanceFactory(String...)}, or
     *      {@link #TestHazelcastInstanceFactory(int)}, are included.
     * <li> The addresses of instances that are not yet started are included.
     * <li> The addresses of shut down instances are included unless they are cleaned
     *      up via {@link #cleanup()}.
     * </ol>
     * In real network: <ol>
     * <li> Only the addresses of running instances are returned.
     * <li> The method is blocked until all created instances are started.
     */
    public Collection<Address> getKnownAddresses() {
        if (isMockNetwork) {
            return unmodifiableCollection(addressMap.values());
        }
        return Hazelcast.getAllHazelcastInstances().stream()
                .map(Accessors::getAddress).toList();
    }

    /**
     * Returns the Hazelcast instance that has the specified address if any;
     * otherwise, returns null. In mock network, the instances that are not yet
     * started are considered. In real network, the method is blocked until all
     * instances are started.
     */
    @Nullable
    public HazelcastInstance getInstance(Address address) {
        if (isMockNetwork) {
            return registry.getInstance(address);
        }
        return Hazelcast.getAllHazelcastInstances().stream()
                .filter(instance -> getAddress(instance).equals(address))
                .findAny().orElse(null);
    }

    /**
     * Shutdown all instances started by this factory.
     */
    public void shutdownAll() {
        if (isMockNetwork) {
            registry.shutdown();
            addressMap.clear();
        } else {
            Hazelcast.shutdownAll();
        }
    }

    /**
     * Terminates all instances started by this factory.
     */
    public void terminateAll() {
        if (isMockNetwork) {
            registry.terminate();
        } else {
            HazelcastInstanceFactory.terminateAll();
        }
    }

    @Override
    public String toString() {
        return "TestHazelcastInstanceFactory{addresses=" + addressMap.values() + '}';
    }

    private Address addNewAddressToAddressMap(int id, String host, int initialPort) {
        synchronized (addressMap) {
            while (true) {
                int newPort = initialPort++;

                if (newPort > MAX_PORT_NUMBER) {
                    throw new IllegalArgumentException(
                            "Exhausted available port range. Try lowering the initial port in "
                                    + getClass().getSimpleName() + ": " + newPort);
                }

                final Address newAddress = createAddressOrNull(host, newPort);
                if (!addressMap.containsValue(newAddress)) {
                    addressMap.put(id, newAddress);
                    return newAddress;
                }
            }
        }
    }

    /**
     * Returns the address with the given {@code host} and {@code port}.
     * This method may return {@code null} if no IP address for the {@code host}
     * could be found.
     */
    private static Address createAddressOrNull(String host, int port) {
        try {
            return new Address(host, port);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Returns a list of addresses with the {@code 127.0.0.1} host and starting
     * with the {@value #DEFAULT_INITIAL_PORT} port or an empty list in case mock
     * network is not used or the requested count is {@code 0}.
     *
     * @param count the number of requested addresses
     */
    private void fillAddressMap(int count) {
        final String[] addresses = new String[count];
        Arrays.fill(addresses, "127.0.0.1");
        fillAddressMap(DEFAULT_INITIAL_PORT, addresses);
    }

    /**
     * Fills the {@link #addressMap} with a list of addresses with the provided
     * hosts and starting with the provided {@code initialPort} port.
     *
     * @param initialPort the initial port for the returned addresses
     * @param hostArray   the array with the address hostnames
     */
    private void fillAddressMap(int initialPort, String... hostArray) {
        if (!isMockNetwork || hostArray.length == 0) {
            return;
        }
        checkElementsNotNull(hostArray);

        int port = initialPort == -1 ? DEFAULT_INITIAL_PORT : initialPort;
        int nodeIndex = 0;
        for (String host : hostArray) {
            addNewAddressToAddressMap(nodeIndex++, host, port++);
        }
    }

    /**
     * Asserts that the array and all of its elements are non-null.
     *
     * @param array the array
     * @param <T>   the type of the array elements
     * @throws NullPointerException if the array or any of its elements are {@code null}.
     */
    private static <T> void checkElementsNotNull(T[] array) {
        checkNotNull(array, "Array should not be null");
        for (Object element : array) {
            checkNotNull(element, "Array element should not be null");
        }
    }

    private static Config initOrCreateConfig(Config config) {
        if (config == null) {
            if (System.getProperty(SYSPROP_MEMBER_CONFIG) != null
                    && isAcceptedSuffixConfigured(System.getProperty(SYSPROP_MEMBER_CONFIG), YAML_ACCEPTED_SUFFIXES)
            ) {
                config = new YamlConfigBuilder().build();
            } else {
                config = new XmlConfigBuilder().build();
            }
            config.getJetConfig().setEnabled(true);
        }
        config.setProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
        String gracefulShutdownMaxWaitValue = System.getProperty(ClusterProperty.GRACEFUL_SHUTDOWN_MAX_WAIT.getName(), "120");
        config.setProperty(ClusterProperty.GRACEFUL_SHUTDOWN_MAX_WAIT.getName(), gracefulShutdownMaxWaitValue);
        if (config.getProperty(ClusterProperty.PARTITION_BACKUP_SYNC_INTERVAL.getName()) == null) {
            config.setProperty(ClusterProperty.PARTITION_BACKUP_SYNC_INTERVAL.getName(), "1");
        }
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        return config;
    }

    /**
     * Removes hazelcast instance address mappings for shut down hazelcast
     * instances. This allows an address to be reused.
     */
    public void cleanup() {
        if (!isMockNetwork) {
            return;
        }
        synchronized (addressMap) {
            addressMap.entrySet().removeIf(entry -> registry.getInstance(entry.getValue()) == null);
        }
    }

    public static class FirewallingNodeContext extends DelegatingNodeContext {

        public FirewallingNodeContext(NodeContext delegate) {
            super(delegate);
        }

        @Override
        public Server createServer(Node node, ServerSocketRegistry registry, LocalAddressRegistry addressRegistry) {
            Server server = super.createServer(node, registry, addressRegistry);
            return new FirewallingServer(server, emptySet());
        }
    }

    public static class NodeExtensionCustomizer extends DelegatingNodeContext {
        private final Function<Node, NodeExtension> nodeExtensionFn;

        NodeExtensionCustomizer(NodeContext delegate, Function<Node, NodeExtension> nodeExtensionFn) {
            super(delegate);
            this.nodeExtensionFn = nodeExtensionFn;
        }

        @Override
        public NodeExtension createNodeExtension(Node node) {
            return nodeExtensionFn.apply(node);
        }
    }

    public abstract static class DelegatingNodeContext implements NodeContext {
        private final NodeContext delegate;

        public DelegatingNodeContext(NodeContext delegate) {
            this.delegate = delegate;
        }

        @Override
        public NodeExtension createNodeExtension(Node node) {
            return delegate.createNodeExtension(node);
        }

        @Override
        public AddressPicker createAddressPicker(Node node) {
            return delegate.createAddressPicker(node);
        }

        @Override
        public Joiner createJoiner(Node node) {
            return delegate.createJoiner(node);
        }

        @Override
        public Server createServer(Node node, ServerSocketRegistry registry, LocalAddressRegistry addressRegistry) {
            return delegate.createServer(node, registry, addressRegistry);
        }
    }
}
