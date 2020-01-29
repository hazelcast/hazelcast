/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.hazelcast.gcp;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class GcpClientTest {
    private static final String CURRENT_PROJECT = "project-1";
    private static final String PROJECT_1 = "project-2";
    private static final String PROJECT_2 = "project-3";
    private static final String CURRENT_ZONE = "us-east1-a";
    private static final String ZONE_1 = "us-east1-b";
    private static final String ZONE_2 = "us-east1-c";
    private static final String ACCESS_TOKEN = "ya29.c.Elr6BVAeC2CeahNthgBf6Nn8j66IfIfZV6eb0LTkDeoAzELseUL5pFmfq0K_ViJN8BaeVB6b16NNCiPB0YbWPnoHRC2I1ghmnknUTzL36t-79b_OitEF_q_C1GM";
    private static final String PRIVATE_KEY_PATH = "/sample/filesystem/path";

    private static final GcpAddress ADDRESS_1 = new GcpAddress("10.240.0.2", "35.207.0.219");
    private static final GcpAddress ADDRESS_2 = new GcpAddress("10.240.0.3", "35.237.227.147");
    private static final GcpAddress ADDRESS_3 = new GcpAddress("10.240.0.4", "35.237.227.148");
    private static final GcpAddress ADDRESS_4 = new GcpAddress("10.240.0.5", "35.237.227.149");
    private static final List<GcpAddress> ADDRESSES = asList(ADDRESS_1, ADDRESS_2, ADDRESS_3, ADDRESS_4);

    @Mock
    private GcpMetadataApi gcpMetadataApi;
    @Mock
    private GcpComputeApi gcpComputeApi;
    @Mock
    private GcpAuthenticator gcpAuthenticator;

    @Before
    public void setUp() {
        when(gcpMetadataApi.currentProject()).thenReturn(CURRENT_PROJECT);
        when(gcpMetadataApi.currentZone()).thenReturn(CURRENT_ZONE);
        when(gcpMetadataApi.accessToken()).thenReturn(ACCESS_TOKEN);
    }

    @Test
    public void getAddressesCurrentProjectCurrentZoneNoLabel() {
        // given
        Label label = null;
        given(gcpComputeApi.instances(CURRENT_PROJECT, CURRENT_ZONE, label, ACCESS_TOKEN)).willReturn(ADDRESSES);

        GcpConfig gcpConfig = GcpConfig.builder().build();
        GcpClient gcpClient = new GcpClient(gcpMetadataApi, gcpComputeApi, gcpAuthenticator, gcpConfig);

        // when
        List<GcpAddress> result = gcpClient.getAddresses();

        // then
        assertEquals(ADDRESSES, result);
    }

    @Test
    public void getAddressesCurrentProjectCurrentZoneWithLabel() {
        // given
        Label label = new Label("application=hazelcast");
        given(gcpComputeApi.instances(CURRENT_PROJECT, CURRENT_ZONE, label, ACCESS_TOKEN)).willReturn(ADDRESSES);

        GcpConfig gcpConfig = GcpConfig.builder().setLabel(label).build();
        GcpClient gcpClient = new GcpClient(gcpMetadataApi, gcpComputeApi, gcpAuthenticator, gcpConfig);

        // when
        List<GcpAddress> result = gcpClient.getAddresses();

        // then
        assertEquals(ADDRESSES, result);
    }

    @Test
    public void getAddressesMultipleProjectsMultipleZones() {
        // given
        Label label = new Label("application=hazelcast");
        given(gcpComputeApi.instances(PROJECT_1, ZONE_1, label, ACCESS_TOKEN)).willReturn(asList(ADDRESS_1));
        given(gcpComputeApi.instances(PROJECT_1, ZONE_2, label, ACCESS_TOKEN)).willReturn(asList(ADDRESS_2));
        given(gcpComputeApi.instances(PROJECT_2, ZONE_1, label, ACCESS_TOKEN)).willReturn(asList(ADDRESS_3));
        given(gcpComputeApi.instances(PROJECT_2, ZONE_2, label, ACCESS_TOKEN)).willReturn(asList(ADDRESS_4));

        GcpConfig gcpConfig = GcpConfig.builder()
                                       .setProjects(asList(PROJECT_1, PROJECT_2))
                                       .setZones(asList(ZONE_1, ZONE_2))
                                       .setLabel(label)
                                       .build();
        GcpClient gcpClient = new GcpClient(gcpMetadataApi, gcpComputeApi, gcpAuthenticator, gcpConfig);

        // when
        List<GcpAddress> result = gcpClient.getAddresses();

        // then
        assertEquals(ADDRESSES, result);
    }

    @Test
    public void getAddressesWithPrivateKeyPath() {
        // given
        given(gcpMetadataApi.accessToken()).willReturn(null);
        given(gcpAuthenticator.refreshAccessToken(PRIVATE_KEY_PATH)).willReturn(ACCESS_TOKEN);
        given(gcpComputeApi.instances(CURRENT_PROJECT, CURRENT_ZONE, null, ACCESS_TOKEN)).willReturn(ADDRESSES);

        GcpConfig gcpConfig = GcpConfig.builder().setPrivateKeyPath(PRIVATE_KEY_PATH).build();
        GcpClient gcpClient = new GcpClient(gcpMetadataApi, gcpComputeApi, gcpAuthenticator, gcpConfig);

        // when
        List<GcpAddress> result = gcpClient.getAddresses();

        // then
        assertEquals(ADDRESSES, result);
    }

    @Test
    public void getAvailabilityZone() {
        // given
        given(gcpMetadataApi.currentZone()).willReturn(ZONE_1);
        GcpConfig gcpConfig = GcpConfig.builder().build();
        GcpClient gcpClient = new GcpClient(gcpMetadataApi, gcpComputeApi, gcpAuthenticator, gcpConfig);

        // when
        String result = gcpClient.getAvailabilityZone();

        // then
        assertEquals(ZONE_1, result);
    }

}