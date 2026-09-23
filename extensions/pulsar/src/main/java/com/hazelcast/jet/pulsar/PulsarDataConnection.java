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
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;

import static com.hazelcast.internal.util.Memoizers.memoizeConcurrent;
import static com.hazelcast.internal.util.Preconditions.checkState;
import static java.util.Collections.emptyList;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

/**
 * Represents Apache Pulsar {@link com.hazelcast.dataconnection.DataConnection}.
 *
 * @since 6.0
 */
public class PulsarDataConnection extends DataConnectionBase {
    private static final ILogger LOG = Logger.getLogger(PulsarDataConnection.class);
    private volatile ConcurrentMemoizingSupplier<PulsarClient> pulsarClientSup;
    private final String brokerUrl;
    private final String serviceHttpUrl;
    private final String namespace;
    private final String tenant;
    private volatile boolean destroyed;

    public PulsarDataConnection(DataConnectionConfig config) {
        super(config);
        brokerUrl = config.getProperty(Properties.BROKER_URL);
        serviceHttpUrl = config.getProperty(Properties.HTTP_SERVICE_URL);
        tenant = config.getProperty(Properties.TENANT);
        namespace = config.getProperty(Properties.NAMESPACE);
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
     * Properties that are possible to use with {@link PulsarDataConnection}.
     */
    public static final class Properties {
        /**
         * Pulsar Broker URL. Always mandatory.
         */
        public static final String BROKER_URL = "brokerUrl";
        /**
         * Pulsar HTTP endpoint URL for administrative tasks, like listing resources.
         */
        public static final String HTTP_SERVICE_URL = "httpServiceUrl";
        /**
         * Either localName ("namespace") or full name ("tenant/namespace").
         * Optional property. If set, it will filter only resources assigned to this namespace.
         */
        public static final String NAMESPACE = "namespace";
        /**
         * Tenant name, optional. If set, it will filter only resources assigned to this tenant.
         */
        public static final String TENANT = "tenant";
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
        try (PulsarAdmin admin = PulsarAdmin.builder()
                                            .serviceHttpUrl(serviceHttpUrl)
                                            .build()) {
            Topics topics = admin.topics();
            List<String> tenantsNames = isBlank(tenant) ? admin.tenants().getTenants() : List.of(tenant);
            for (String tenant : tenantsNames) {
                List<String> namespaceNames = possibleNamespaceNames(admin, tenant);
                for (String namespace : namespaceNames) {
                    List<String> allTopicNames = topics.getList(namespace);
                    for (String topic : allTopicNames) {
                        var nameParts = TopicName.get(topic).getSchemaName().split("/");
                        resources.add(new DataConnectionResource("Topic", nameParts));
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
                try {
                    ((CloseablePulsarClient) client).unwrap().close();
                } catch (PulsarClientException e) {
                    throw new HazelcastException("Error while trying to close PulsarClient by PulsarDataConnection", e);
                }
            }
        }
    }

    @Nonnull
    private PulsarClient createClient() {
        try {
            return PulsarClient.builder()
                               .serviceUrl(brokerUrl)
                               .build();
        } catch (PulsarClientException e) {
            throw new HazelcastException("Error while trying to create PulsarClient", e);
        }
    }

    /**
     * Returns basic {@link DataConnectionConfig} with given name, brokerUrl and service url.
     */
    @Nonnull
    public static DataConnectionConfig pulsarDataConnectionConf(String name, String brokerUrl, String serviceHttpUrl) {
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig();
        dataConnectionConfig.setName(name);
        dataConnectionConfig.setShared(true);
        dataConnectionConfig.setProperty(Properties.BROKER_URL, brokerUrl);
        if (isNotBlank(serviceHttpUrl)) {
            dataConnectionConfig.setProperty(Properties.HTTP_SERVICE_URL, serviceHttpUrl);
        }
        dataConnectionConfig.setType("Pulsar");
        return dataConnectionConfig;
    }

}
