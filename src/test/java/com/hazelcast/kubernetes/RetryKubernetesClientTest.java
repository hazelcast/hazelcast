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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RetryKubernetesClientTest {
    private static final int RETRIES = 1;

    private static final String NAMESPACE = "someNamespace";
    private static final String SERVICE_LABEL = "someServiceLabel";
    private static final String SERVICE_LABEL_VALUE = "someServiceLabelValue";
    private static final String SERVICE_NAME = "someServiceName";

    private static final Endpoints ENDPOINTS = new Endpoints(null, null);

    @Mock
    KubernetesClient mockClient;

    private RetryKubernetesClient client;

    @Before
    public void setUp() {
        client = new RetryKubernetesClient(mockClient, RETRIES);

        when(mockClient.endpoints(anyString())).thenReturn(ENDPOINTS);
        when(mockClient.endpointsByLabel(anyString(), anyString(), anyString())).thenReturn(ENDPOINTS);
        when(mockClient.endpointsByName(anyString(), anyString())).thenReturn(ENDPOINTS);
    }

    @Test
    public void endpoints() {
        // given
        given(mockClient.endpoints(NAMESPACE)).willThrow(KubernetesClientException.class).willReturn(ENDPOINTS);

        // when
        Endpoints result = client.endpoints(NAMESPACE);

        // then
        assertEquals(ENDPOINTS, result);
    }

    @Test(expected = KubernetesClientException.class)
    public void endpointsRetriesExceeded() {
        // given
        given(mockClient.endpoints(NAMESPACE)).willThrow(KubernetesClientException.class)
                                              .willThrow(KubernetesClientException.class).willReturn(ENDPOINTS);

        // when
        Endpoints result = client.endpoints(NAMESPACE);

        // then
        // throws exception
    }

    @Test
    public void endpointsByLabel() {
        // given
        given(mockClient.endpointsByLabel(NAMESPACE, SERVICE_LABEL, SERVICE_LABEL_VALUE))
                .willThrow(KubernetesClientException.class).willReturn(ENDPOINTS);

        // when
        Endpoints result = client.endpointsByLabel(NAMESPACE, SERVICE_LABEL, SERVICE_LABEL_VALUE);

        // then
        assertEquals(ENDPOINTS, result);
    }

    @Test(expected = KubernetesClientException.class)
    public void endpointsByLabelRetriesExceeded() {
        // given
        given(mockClient.endpointsByLabel(NAMESPACE, SERVICE_LABEL, SERVICE_LABEL_VALUE))
                .willThrow(KubernetesClientException.class)
                .willThrow(KubernetesClientException.class).willReturn(ENDPOINTS);

        // when
        Endpoints result = client.endpointsByLabel(NAMESPACE, SERVICE_LABEL, SERVICE_LABEL_VALUE);

        // then
        // throws exception
    }

    @Test
    public void endpointsByName() {
        // given
        given(mockClient.endpointsByName(NAMESPACE, SERVICE_NAME))
                .willThrow(KubernetesClientException.class).willReturn(ENDPOINTS);

        // when
        Endpoints result = client.endpointsByName(NAMESPACE, SERVICE_NAME);

        // then
        assertEquals(ENDPOINTS, result);
    }

    @Test(expected = KubernetesClientException.class)
    public void endpointsByNameRetriesExceeded() {
        // given
        given(mockClient.endpointsByName(NAMESPACE, SERVICE_NAME))
                .willThrow(KubernetesClientException.class)
                .willThrow(KubernetesClientException.class).willReturn(ENDPOINTS);

        // when
        Endpoints result = client.endpointsByName(NAMESPACE, SERVICE_NAME);

        // then
        // throws exception
    }
}