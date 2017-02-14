package com.hazelcast.kubernetes;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.NoLogFactory;
import com.hazelcast.spi.discovery.DiscoveryNode;
import io.fabric8.kubernetes.api.model.EndpointAddress;
import io.fabric8.kubernetes.api.model.EndpointSubset;
import io.fabric8.kubernetes.api.model.Endpoints;
import io.fabric8.kubernetes.api.model.EndpointsList;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.dsl.ClientMixedOperation;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(ServiceEndpointResolver.class)
/**
 * This test class sent by https://github.com/hazelcast/hazelcast-kubernetes/pull/30
 */
public class ServiceEndpointResolverTest {
    private static final ILogger LOGGER = new NoLogFactory().getLogger("no");
    private static final String SERVICE_NAME = "";
    private static final String SERVICE_LABEL = "theLabel";
    private static final String SERVICE_LABEL_VALUE = "serviceLabelValue";
    private static final String NAMESPACE = "theNamespace";
    private static final String KUBERNETES_MASTER_URL = "http://bla";
    private static final String API_TOKEN = "token";

    @Mock
    private DefaultKubernetesClient client;

    @Mock
    private ClientMixedOperation endpoints;

    @Mock
    private ClientMixedOperation inNamespace;

    @Mock
    private ClientMixedOperation withLabel;

    private EndpointsList nodesInNamespace = new EndpointsList();
    private EndpointsList nodesWithLabel = new EndpointsList();

    @Before
    public void setup() throws Exception {
        PowerMockito.whenNew(DefaultKubernetesClient.class).withAnyArguments().thenReturn(client);

        when(client.endpoints()).thenReturn(endpoints);
        when(endpoints.inNamespace(NAMESPACE)).thenReturn(inNamespace);
        when(inNamespace.list()).thenReturn(nodesInNamespace);
        when(inNamespace.withLabel(SERVICE_LABEL, SERVICE_LABEL_VALUE)).thenReturn(withLabel);
        when(withLabel.list()).thenReturn(nodesWithLabel);
    }

    @Test
    public void resolveWithNamespaceAndNoNodeInNamespace() {
        ServiceEndpointResolver sut = new ServiceEndpointResolver(LOGGER, SERVICE_NAME, null, null, NAMESPACE, KUBERNETES_MASTER_URL, API_TOKEN);
        List<DiscoveryNode> nodes = sut.resolve();

        assertEquals(0, nodes.size());
    }

    @Test
    public void resolveWithNamespaceAndNodeInNamespace() {
        Endpoints discoveryNode = createEndpoints(1);
        nodesInNamespace.getItems().add(discoveryNode);

        ServiceEndpointResolver sut = new ServiceEndpointResolver(LOGGER, SERVICE_NAME, null, null, NAMESPACE, KUBERNETES_MASTER_URL, API_TOKEN);
        List<DiscoveryNode> nodes = sut.resolve();

        assertEquals(1, nodes.size());
        assertEquals(1, nodes.get(0).getPrivateAddress().getPort());
    }

    @Test
    public void resolveWithServiceLabelAndNodeInNamespace() {
        nodesInNamespace.getItems().add(createEndpoints(1));

        ServiceEndpointResolver sut = new ServiceEndpointResolver(LOGGER, SERVICE_NAME, SERVICE_LABEL, SERVICE_LABEL_VALUE, NAMESPACE, KUBERNETES_MASTER_URL, API_TOKEN);
        List<DiscoveryNode> nodes = sut.resolve();

        assertEquals(0, nodes.size());
    }

    @Test
    public void resolveWithServiceLabelAndNodeWithServiceLabel() {
        nodesInNamespace.getItems().add(createEndpoints(1));
        Endpoints discoveryNode = createEndpoints(2);
        nodesWithLabel.getItems().add(discoveryNode);

        ServiceEndpointResolver sut = new ServiceEndpointResolver(LOGGER, SERVICE_NAME, SERVICE_LABEL, SERVICE_LABEL_VALUE, NAMESPACE, KUBERNETES_MASTER_URL, API_TOKEN);
        List<DiscoveryNode> nodes = sut.resolve();

        assertEquals(1, nodes.size());
        assertEquals(2, nodes.get(0).getPrivateAddress().getPort());
    }

    private Endpoints createEndpoints(int id) {
        Endpoints endpoints = new Endpoints();
        EndpointSubset subset = new EndpointSubset();
        endpoints.getSubsets().add(subset);
        EndpointAddress address = new EndpointAddress();
        subset.getAddresses().add(address);
        address.setIp("1.1.1.1");
        address.getAdditionalProperties().put("hazelcast-service-port", String.valueOf(id));
        return endpoints;
    }
}