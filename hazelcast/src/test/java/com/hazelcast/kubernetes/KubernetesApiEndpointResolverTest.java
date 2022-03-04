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

package com.hazelcast.kubernetes;

import com.hazelcast.kubernetes.KubernetesClient.Endpoint;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.NoLogFactory;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class KubernetesApiEndpointResolverTest {
    private static final ILogger LOGGER = new NoLogFactory().getLogger("no");
    private static final String SERVICE_NAME = "serviceName";
    private static final String SERVICE_LABEL = "serviceLabel";
    private static final String SERVICE_LABEL_VALUE = "serviceLabelValue";
    private static final String POD_LABEL = "podLabel";
    private static final String POD_LABEL_VALUE = "podLabelValue";
    private static final Boolean RESOLVE_NOT_READY_ADDRESSES = true;

    private final KubernetesClient client = mock(KubernetesClient.class);

    @Test
    public void resolveWhenNodeInFound() {
        // given
        List<Endpoint> endpoints = Collections.<Endpoint>emptyList();
        given(client.endpoints()).willReturn(endpoints);

        KubernetesApiEndpointResolver sut = new KubernetesApiEndpointResolver(LOGGER, null, 0, null, null, null, null, null, client);

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
        List<Endpoint> endpoints = createEndpoints(1);
        given(client.endpointsByName(SERVICE_NAME)).willReturn(endpoints);

        KubernetesApiEndpointResolver sut = new KubernetesApiEndpointResolver(LOGGER, SERVICE_NAME, port, null, null, null, null, null,
                client);

        // when
        List<DiscoveryNode> nodes = sut.resolve();

        // then
        assertEquals(1, nodes.size());
        assertEquals(expectedPort, nodes.get(0).getPrivateAddress().getPort());
    }

    @Test
    public void resolveWithServiceLabelWhenNodeWithServiceLabel() {
        // given
        List<Endpoint> endpoints = createEndpoints(2);
        given(client.endpointsByServiceLabel(SERVICE_LABEL, SERVICE_LABEL_VALUE)).willReturn(endpoints);

        KubernetesApiEndpointResolver sut = new KubernetesApiEndpointResolver(LOGGER, null, 0, SERVICE_LABEL, SERVICE_LABEL_VALUE,
                null, null, null, client);

        // when
        List<DiscoveryNode> nodes = sut.resolve();

        // then
        assertEquals(1, nodes.size());
        assertEquals(2, nodes.get(0).getPrivateAddress().getPort());
    }

    @Test
    public void resolveWithPodLabelWhenNodeWithPodLabel() {
        // given
        List<Endpoint> endpoints = createEndpoints(2);
        given(client.endpointsByPodLabel(POD_LABEL, POD_LABEL_VALUE)).willReturn(endpoints);

        KubernetesApiEndpointResolver sut = new KubernetesApiEndpointResolver(LOGGER, null, 0, null, null,
                POD_LABEL, POD_LABEL_VALUE, null, client);

        // when
        List<DiscoveryNode> nodes = sut.resolve();

        // then
        assertEquals(1, nodes.size());
        assertEquals(2, nodes.get(0).getPrivateAddress().getPort());
    }

    @Test
    public void resolveWithServiceNameWhenNotReadyAddressesAndNotReadyEnabled() {
        // given
        List<Endpoint> endpoints = createNotReadyEndpoints(2);
        given(client.endpointsByName(SERVICE_NAME)).willReturn(endpoints);

        KubernetesApiEndpointResolver sut = new KubernetesApiEndpointResolver(LOGGER, SERVICE_NAME, 0, null, null,
                null, null, RESOLVE_NOT_READY_ADDRESSES, client);

        // when
        List<DiscoveryNode> nodes = sut.resolve();

        // then
        assertEquals(1, nodes.size());
    }

    @Test
    public void resolveWithServiceNameWhenNotReadyAddressesAndNotReadyDisabled() {
        // given
        List<Endpoint> endpoints = createNotReadyEndpoints(2);
        given(client.endpointsByName(SERVICE_NAME)).willReturn(endpoints);

        KubernetesApiEndpointResolver sut = new KubernetesApiEndpointResolver(LOGGER, SERVICE_NAME, 0, null, null, null, null, null,
                client);

        // when
        List<DiscoveryNode> nodes = sut.resolve();

        // then
        assertEquals(0, nodes.size());
    }

    private static List<Endpoint> createEndpoints(int customPort) {
        return asList(createEntrypointAddress(customPort, true));
    }

    private static List<Endpoint> createNotReadyEndpoints(int customPort) {
        return asList(createEntrypointAddress(customPort, false));
    }

    private static Endpoint createEntrypointAddress(int customPort, boolean isReady) {
        String ip = "1.1.1.1";
        return new Endpoint(new KubernetesClient.EndpointAddress(ip, customPort), isReady);
    }
}
