/*
 * Copyright 2026 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.pulsar;

import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.dataconnection.DataConnectionBase;
import com.hazelcast.dataconnection.DataConnectionResource;
import com.hazelcast.internal.util.concurrent.ConcurrentMemoizingSupplier;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.shade.org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.shade.org.apache.pulsar.common.naming.TopicName;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.internal.util.Memoizers.memoizeConcurrent;
import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static com.hazelcast.internal.util.Preconditions.checkState;
import static com.hazelcast.jet.pulsar.PulsarProperties.clientOption;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNullElse;
import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * Represents Apache Pulsar {@link com.hazelcast.dataconnection.DataConnection}.
 *
 * @since 6.0
 */
public class PulsarDataConnection extends DataConnectionBase {
    private static final ILogger LOG = Logger.getLogger(PulsarDataConnection.class);
    private static final long DEFAULT_CLOSE_TIMEOUT_SECONDS = 300L;
    private volatile ConcurrentMemoizingSupplier<PulsarClient> pulsarClientSup;
    private final String brokerUrl;
    private final String serviceHttpUrl;
    private final String namespace;
    private final String tenant;
    private final boolean enableTransactions;
    private final Duration closeTimeout;
    private volatile boolean destroyed;

    public PulsarDataConnection(DataConnectionConfig config) {
        super(config);
        var options = options();
        brokerUrl = clientOption(PulsarProperties.BROKER_URL, options);
        serviceHttpUrl = clientOption(PulsarProperties.HTTP_SERVICE_URL, options);
        if (isBlank(brokerUrl)) {
            throw new IllegalArgumentException("Pulsar Broker URL is required");
        }
        if (isBlank(serviceHttpUrl)) {
            throw new IllegalArgumentException("Pulsar HTTP Service URL is required");
        }
        tenant = clientOption(PulsarProperties.TENANT, options);
        namespace = clientOption(PulsarProperties.NAMESPACE, options);
        enableTransactions = Boolean.parseBoolean(
            requireNonNullElse(clientOption(PulsarProperties.ENABLE_TRANSACTIONS, options), "false"));
        closeTimeout = Duration.ofSeconds(checkPositive(PulsarProperties.CLOSE_TIMEOUT_SECONDS,
                                                        Long.parseLong(requireNonNullElse(
                                                            clientOption(PulsarProperties.CLOSE_TIMEOUT_SECONDS, options),
                                                            String.valueOf(DEFAULT_CLOSE_TIMEOUT_SECONDS)))));
        if (config.isShared()) {
            pulsarClientSup = memoizeConcurrent(
                    () -> new CloseablePulsarClient(createClient(),
                                                    this::release,
                                                    this::release,
                                                    this::isDestroyed));
        }
    }

    boolean isDestroyed() {
        return destroyed;
    }

    /**
     * Creates a new {@link PulsarClient client} or reuses shared one.
     * <p>
     * In case of shared data connections, the {@link #retain()} method is always automatically called.
     * If there is an error during client creation, the data connection is not released and {@link #release()}
     * must be called explicitly.
     */
    @Nonnull
    public PulsarClient getClient() {
        if (getConfig().isShared()) {
            retain();
            // local copy to protect from nullifying the value between two instructions
            ConcurrentMemoizingSupplier<PulsarClient> supplier = pulsarClientSup;
            checkState(supplier != null, "Pulsar client should not be closed at this point");
            return supplier.get();
        } else {
            PulsarClient client = createClient();
            return new CloseablePulsarClient(client, client::close, client::shutdown, client::isClosed);
        }
    }

    public PulsarAdmin getAdmin() {
        if (serviceHttpUrl == null) {
            throw new IllegalArgumentException("serviceHttpUrl is required for accessing admin API");
        }
        try {
            return ConnectionSuppliers.createAdmin(serviceHttpUrl, options());
        } catch (PulsarClientException e) {
            throw new HazelcastException(e);
        }
    }

    /**
     * Lists all resources, filtering out by tenant and/or namespace if specified.
     */
    @Nonnull
    @Override
    public Collection<DataConnectionResource> listResources() {
        if (serviceHttpUrl == null) {
            throw new HazelcastException("serviceHttpUrl is required to list Pulsar topics");
        }
        var resources = new LinkedHashSet<DataConnectionResource>();
        try (PulsarAdmin admin = ConnectionSuppliers.createAdmin(serviceHttpUrl, options())) {
            Topics topics = admin.topics();
            List<String> tenantsNames = isBlank(tenant) ? admin.tenants().getTenants() : List.of(tenant);
            for (String tenant : tenantsNames) {
                List<String> namespaceNames = possibleNamespaceNames(admin, tenant);
                for (String namespace : namespaceNames) {
                    Set<String> allTopicNames = new LinkedHashSet<>(topics.getList(namespace));
                    // non-persistent topics do not appear in "getList"
                    allTopicNames.addAll(topics.getPartitionedTopicList(namespace));
                    for (String topic : allTopicNames) {
                        TopicName name = TopicName.get(topic);
                        var nameParts = new ArrayList<>(Arrays.asList(name.getSchemaName().split("/")));
                        nameParts.add(0, name.getDomain().toString());
                        resources.add(new DataConnectionResource("Topic", nameParts.toArray(new String[0])));
                    }
                }
            }
            return List.copyOf(resources);
        } catch (PulsarClientException | PulsarAdminException e) {
            throw new HazelcastException("Unable to list resources", e);
        }
    }

    private List<String> possibleNamespaceNames(PulsarAdmin admin, String tenant) throws PulsarAdminException {
        Namespaces namespaces = admin.namespaces();
        List<String> allNamespaces = namespaces.getNamespaces(tenant);
        if (isBlank(namespace)) {
            return allNamespaces;
        } else {
            NamespaceName fullNamespaceName = allNamespaces.stream()
                                                           .map(NamespaceName::get)
                                                           .filter(n -> n.getLocalName().equals(namespace)
                                                                            || n.toString().equals(namespace))
                                                           .findFirst()
                                                           .orElse(null);
            if (fullNamespaceName != null) {
                return List.of(fullNamespaceName.toString());
            } else {
                LOG.warning("Namespace '" + namespace + "' does not exist for tenant '" + tenant + "'");
                return emptyList();
            }
        }
    }

    @Nonnull
    @Override
    public Collection<String> resourceTypes() {
        return List.of("Topic");
    }

    @Override
    public void destroy() {
        destroyed = true;
        ConcurrentMemoizingSupplier<PulsarClient> supplier = pulsarClientSup;
        if (supplier != null) {
            pulsarClientSup = null;
            PulsarClient client = supplier.remembered();
            if (client != null) {
                PulsarClient unwrapped = ((CloseablePulsarClient) client).unwrap();
                try {
                    unwrapped.closeAsync().get(closeTimeout.toMillis(), TimeUnit.MILLISECONDS);
                } catch (ExecutionException | InterruptedException | TimeoutException e) {
                    if (e instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                    try {
                        unwrapped.shutdown();
                    } catch (PulsarClientException ex) {
                        e.addSuppressed(ex);
                        throw new HazelcastException("Error while trying to close PulsarClient by PulsarDataConnection", e);
                    }
                }
            }
        }
    }

    @Nonnull
    private PulsarClient createClient() {
        try {
            return ConnectionSuppliers.createClient(brokerUrl, enableTransactions, Map.copyOf(options()));
        } catch (PulsarClientException e) {
            throw new HazelcastException("Error while trying to create PulsarClient", e);
        }
    }

    /**
     * Returns basic {@link DataConnectionConfig} with given name, brokerUrl and service url.
     */
    @Nonnull
    public static DataConnectionConfig pulsarDataConnectionConf(@Nonnull String name, @Nonnull String brokerUrl,
                                                                @Nullable String serviceHttpUrl) {
        return pulsarDataConnectionConf(name, brokerUrl, serviceHttpUrl, false);
    }

    /**
     * Returns basic {@link DataConnectionConfig} with given name, brokerUrl and service url
     * and transactions activated/deactivated.
     */
    @Nonnull
    public static DataConnectionConfig pulsarDataConnectionConf(@Nonnull String name,
                                                                @Nonnull String brokerUrl,
                                                                @Nullable String serviceHttpUrl,
                                                                boolean enableTransactions) {
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig();
        dataConnectionConfig.setName(name);
        dataConnectionConfig.setShared(true);
        dataConnectionConfig.setProperty(PulsarProperties.BROKER_URL, brokerUrl);
        dataConnectionConfig.setProperty(PulsarProperties.HTTP_SERVICE_URL, serviceHttpUrl);
        dataConnectionConfig.setProperty(PulsarProperties.ENABLE_TRANSACTIONS, String.valueOf(enableTransactions));
        dataConnectionConfig.setType("Pulsar");
        return dataConnectionConfig;
    }

}
