/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.kubernetes;

import com.hazelcast.kubernetes.KubernetesClient.Endpoints;
import com.hazelcast.kubernetes.KubernetesClient.EntrypointAddress;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.NoLogFactory;
import com.hazelcast.spi.discovery.DiscoveryNode;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;

@RunWith(PowerMockRunner.class)
@PrepareForTest(KubernetesApiEndpointResolver.class)
public class KubernetesApiEndpointResolverTest {
    private static final ILogger LOGGER = new NoLogFactory().getLogger("no");
    private static final String SERVICE_NAME = "serviceName";
    private static final String SERVICE_LABEL = "theLabel";
    private static final String SERVICE_LABEL_VALUE = "serviceLabelValue";
    private static final Boolean RESOLVE_NOT_READY_ADDRESSES = true;
    private static final String NAMESPACE = "theNamespace";

    @Mock
    private RetryKubernetesClient client;

    @Before
    public void setup()
            throws Exception {
        PowerMockito.whenNew(RetryKubernetesClient.class).withAnyArguments().thenReturn(client);
    }

    @Test
    public void resolveWhenNodeInFound() {
        // given
        Endpoints endpoints = new Endpoints(Collections.<EntrypointAddress>emptyList(),
                Collections.<EntrypointAddress>emptyList());
        given(client.endpoints(NAMESPACE)).willReturn(endpoints);

        KubernetesApiEndpointResolver sut = new KubernetesApiEndpointResolver(LOGGER, null, 0, null, null, NAMESPACE, null,
                client);

        // when
        List<DiscoveryNode> nodes = sut.resolve();

        // then
        assertEquals(0, nodes.size());
    }

    @Test
    public void resolveWithServiceNameWhenNodeInNamespace() {
        resolveWithServiceNameWhenNodeInNamespace(0, 1); // expected port 1 is the kubernetes discovery endpoint port
    }

    @Test
    public void resolveWithServiceNameWhenNodeInNamespaceAndCustomPort() {
        resolveWithServiceNameWhenNodeInNamespace(333, 333);
    }

    private void resolveWithServiceNameWhenNodeInNamespace(final int port, final int expectedPort) {
        // given
        Endpoints endpoints = createEndpoints(1);
        given(client.endpointsByName(NAMESPACE, SERVICE_NAME)).willReturn(endpoints);

        KubernetesApiEndpointResolver sut = new KubernetesApiEndpointResolver(LOGGER, SERVICE_NAME, port, null, null, NAMESPACE,
                null, client);

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

        KubernetesApiEndpointResolver sut = new KubernetesApiEndpointResolver(LOGGER, null, 0, SERVICE_LABEL, SERVICE_LABEL_VALUE,
                NAMESPACE, null, client);

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

        KubernetesApiEndpointResolver sut = new KubernetesApiEndpointResolver(LOGGER, SERVICE_NAME, 0, null, null, NAMESPACE,
                RESOLVE_NOT_READY_ADDRESSES, client);

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

        KubernetesApiEndpointResolver sut = new KubernetesApiEndpointResolver(LOGGER, SERVICE_NAME, 0, null, null, NAMESPACE,
                null,
                client);

        // when
        List<DiscoveryNode> nodes = sut.resolve();

        // then
        assertEquals(0, nodes.size());
    }

    private static Endpoints createEndpoints(int customPort) {
        return new Endpoints(asList(createEntrypointAddress(customPort)), Collections.<EntrypointAddress>emptyList());
    }

    private static Endpoints createNotReadyEndpoints(int customPort) {
        return new Endpoints(Collections.<EntrypointAddress>emptyList(), asList(createEntrypointAddress(customPort)));
    }

    private static EntrypointAddress createEntrypointAddress(int customPort) {
        String ip = "1.1.1.1";
        return new EntrypointAddress(ip, customPort, new HashMap<String, Object>());
    }
}