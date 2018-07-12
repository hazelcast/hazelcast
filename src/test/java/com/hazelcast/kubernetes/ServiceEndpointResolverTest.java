package com.hazelcast.kubernetes;

import com.hazelcast.kubernetes.KubernetesClient.Endpoints;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.NoLogFactory;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.spi.discovery.DiscoveryNode;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(ServiceEndpointResolver.class)
/**
 * This test class sent by https://github.com/hazelcast/hazelcast-kubernetes/pull/30
 */ public class ServiceEndpointResolverTest {
    private static final ILogger LOGGER = new NoLogFactory().getLogger("no");
    private static final String SERVICE_NAME = "serviceName";
    private static final String SERVICE_LABEL = "theLabel";
    private static final String SERVICE_LABEL_VALUE = "serviceLabelValue";
    private static final Boolean RESOLVE_NOT_READY_ADDRESSES = true;
    private static final String NAMESPACE = "theNamespace";
    private static final String KUBERNETES_MASTER_URL = "http://bla";
    private static final String API_TOKEN = "token";

    @Mock
    private KubernetesClient client;

    private Endpoints endpoints;

    @Before
    public void setup()
            throws Exception {
        PowerMockito.whenNew(KubernetesClient.class).withAnyArguments().thenReturn(client);

        when(client.endpointsByName(NAMESPACE, SERVICE_NAME)).thenReturn(endpoints);
        when(client.endpointsByLabel(NAMESPACE, SERVICE_LABEL, SERVICE_LABEL_VALUE)).thenReturn(endpoints);
    }

    @Test
    public void resolveWithNamespaceAndServiceNameWhenNodeInFound() {
        // given
        Endpoints endpoints = new Endpoints(Collections.<KubernetesClient.EntrypointAddress>emptyList(),
                Collections.<KubernetesClient.EntrypointAddress>emptyList());
        given(client.endpointsByName(NAMESPACE, SERVICE_NAME)).willReturn(endpoints);

        ServiceEndpointResolver sut = new ServiceEndpointResolver(LOGGER, SERVICE_NAME, 0, null, null, NAMESPACE, null,
                KUBERNETES_MASTER_URL, API_TOKEN);

        // when
        List<DiscoveryNode> nodes = sut.resolve();

        // then
        assertEquals(0, nodes.size());
    }

    @Test
    public void resolveWithNamespaceAndServiceNameWhenNodeInNamespace() {
        resolveWithNamespaceAndServiceNameWhenNodeInNamespace(0, 1); // expected port 1 is the kubernetes discovery endpoint port
    }

    @Test
    public void resolveWithNamespaceAndServiceNameWhenNodeInNamespaceAndCustomPort() {
        resolveWithNamespaceAndServiceNameWhenNodeInNamespace(333, 333);
    }

    private void resolveWithNamespaceAndServiceNameWhenNodeInNamespace(final int port, final int expectedPort) {
        // given
        Endpoints endpoints = createEndpoints(1);
        given(client.endpointsByName(NAMESPACE, SERVICE_NAME)).willReturn(endpoints);

        ServiceEndpointResolver sut = new ServiceEndpointResolver(LOGGER, SERVICE_NAME, port, null, null, NAMESPACE, null,
                KUBERNETES_MASTER_URL, API_TOKEN);

        // when
        List<DiscoveryNode> nodes = sut.resolve();

        // then
        assertEquals(1, nodes.size());
        assertEquals(expectedPort, nodes.get(0).getPrivateAddress().getPort());
    }

    @Test
    public void resolveWithServiceLabelWhenNodeWithServiceLabel() {
        // given
        Endpoints endpoints = createEndpoints(2);
        given(client.endpointsByLabel(NAMESPACE, SERVICE_LABEL, SERVICE_LABEL_VALUE)).willReturn(endpoints);

        ServiceEndpointResolver sut = new ServiceEndpointResolver(LOGGER, null, 0, SERVICE_LABEL, SERVICE_LABEL_VALUE, NAMESPACE,
                null, KUBERNETES_MASTER_URL, API_TOKEN);

        // when
        List<DiscoveryNode> nodes = sut.resolve();

        // then
        assertEquals(1, nodes.size());
        assertEquals(2, nodes.get(0).getPrivateAddress().getPort());
    }

    @Test
    public void resolveWithServiceNameWhenNotReadyAddressesAndNotReadyEnabled() {
        // given
        Endpoints endpoints = createNotReadyEndpoints(2);
        given(client.endpointsByName(NAMESPACE, SERVICE_NAME)).willReturn(endpoints);

        ServiceEndpointResolver sut = new ServiceEndpointResolver(LOGGER, SERVICE_NAME, 0, null, null, NAMESPACE,
                RESOLVE_NOT_READY_ADDRESSES, KUBERNETES_MASTER_URL, API_TOKEN);

        // when
        List<DiscoveryNode> nodes = sut.resolve();

        // then
        assertEquals(1, nodes.size());
    }

    @Test
    public void resolveWithServiceNameWhenNotReadyAddressesAndNotReadyDisabled() {
        // given
        Endpoints endpoints = createNotReadyEndpoints(2);
        given(client.endpointsByName(NAMESPACE, SERVICE_NAME)).willReturn(endpoints);

        ServiceEndpointResolver sut = new ServiceEndpointResolver(LOGGER, SERVICE_NAME, 0, null, null, NAMESPACE, null,
                KUBERNETES_MASTER_URL, API_TOKEN);

        // when
        List<DiscoveryNode> nodes = sut.resolve();

        // then
        assertEquals(0, nodes.size());
    }

    @Test
    public void testReadFileContents()
            throws IOException {
        String expectedContents = "Hello, world!\nThis is a test with Unicode ✓.";
        String testFile = createTestFile(expectedContents);
        String actualContents = ServiceEndpointResolver.readFileContents(testFile);
        Assert.assertEquals(expectedContents, actualContents);
    }

    private String createTestFile(String expectedContents)
            throws IOException {
        File temp = File.createTempFile("test", ".tmp");
        temp.deleteOnExit();
        BufferedWriter bufferedWriter = null;
        try {
            bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(temp), Charset.forName("UTF-8")));
            bufferedWriter.write(expectedContents);
        } finally {
            IOUtil.closeResource(bufferedWriter);
        }
        return temp.getAbsolutePath();
    }

    private Endpoints createEndpoints(int id) {
        String ip = "1.1.1.1";
        Map<String, Object> additionalProperties = new HashMap<String, Object>();
        additionalProperties.put("hazelcast-service-port", String.valueOf(id));
        KubernetesClient.EntrypointAddress address = new KubernetesClient.EntrypointAddress(ip, additionalProperties);
        return new Endpoints(asList(address), Collections.<KubernetesClient.EntrypointAddress>emptyList());
    }

    private Endpoints createNotReadyEndpoints(int id) {
        String ip = "1.1.1.1";
        Map<String, Object> additionalProperties = new HashMap<String, Object>();
        additionalProperties.put("hazelcast-service-port", String.valueOf(id));
        KubernetesClient.EntrypointAddress address = new KubernetesClient.EntrypointAddress(ip, additionalProperties);
        return new Endpoints(Collections.<KubernetesClient.EntrypointAddress>emptyList(), asList(address));
    }
}