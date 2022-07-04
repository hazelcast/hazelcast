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

package com.hazelcast.gcp;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class GcpComputeApiTest {
    private static final String PROJECT = "project1";
    private static final String ZONE = "us-east1-b";
    private static final String REGION = "us-east1";
    private static final String LABEL_KEY = "application";
    private static final String LABEL_VALUE = "hazelcast";
    private static final String ACCESS_TOKEN = "ya29.c.Elr6BVAeC2CeahNthgBf6Nn8j66IfIfZV6eb0LTkDeoAzELseUL5pFmfq0K_ViJN8BaeVB6b16NNCiPB0YbWPnoHRC2I1ghmnknUTzL36t-79b_OitEF_q_C1GM";

    private static final String INSTANCE_1_PRIVATE_IP = "10.240.0.2";
    private static final String INSTANCE_1_PUBLIC_IP = "35.207.0.219";
    private static final String INSTANCE_2_PRIVATE_IP = "10.240.0.3";
    private static final String INSTANCE_2_PUBLIC_IP = "35.237.227.147";
    private static final String INSTANCE_3_PRIVATE_IP = "10.240.0.3";

    private GcpComputeApi gcpComputeApi;

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().dynamicPort());

    @Before
    public void setUp() {
        gcpComputeApi = new GcpComputeApi(String.format("http://localhost:%s", wireMockRule.port()));
    }

    @Test
    public void instances() {
        // given
        stubFor(get(urlEqualTo(
                String.format("/compute/v1/projects/%s/zones/%s/instances?filter=labels.%s+eq+%s", PROJECT, ZONE, LABEL_KEY,
                        LABEL_VALUE)))
                .withHeader("Authorization", equalTo(String.format("OAuth %s", ACCESS_TOKEN)))
                .willReturn(aResponse().withStatus(200).withBody(instancesResponse())));
        Label label = new Label(String.format("%s=%s", LABEL_KEY, LABEL_VALUE));

        // when
        List<GcpAddress> result = gcpComputeApi.instances(PROJECT, ZONE, label, ACCESS_TOKEN);

        // then
        GcpAddress address1 = new GcpAddress(INSTANCE_1_PRIVATE_IP, INSTANCE_1_PUBLIC_IP);
        GcpAddress address2 = new GcpAddress(INSTANCE_2_PRIVATE_IP, INSTANCE_2_PUBLIC_IP);
        assertEquals(asList(address1, address2), result);
    }

    @Test
    public void zones() {
        // given
        stubFor(get(urlEqualTo(
                String.format("/compute/v1/projects/%s/regions/%s?alt=json&fields=zones", PROJECT, REGION)))
                .withHeader("Authorization", equalTo(String.format("OAuth %s", ACCESS_TOKEN)))
                .willReturn(aResponse().withStatus(200).withBody(regionResponse(PROJECT, REGION))));
        // when
        List<String> zones = gcpComputeApi.zones(PROJECT, REGION, ACCESS_TOKEN);

        // then
        String zoneA = REGION + "-a";
        String zoneB = REGION + "-b";
        String zoneC = REGION + "-c";
        assertEquals(asList(zoneA, zoneB, zoneC), zones);
    }

    @Test
    public void instancesNoLabel() {
        // given
        stubFor(get(urlEqualTo(String.format("/compute/v1/projects/%s/zones/%s/instances", PROJECT, ZONE)))
                .withHeader("Authorization", equalTo(String.format("OAuth %s", ACCESS_TOKEN)))
                .willReturn(aResponse().withStatus(200).withBody(instancesResponse())));

        // when
        List<GcpAddress> result = gcpComputeApi.instances(PROJECT, ZONE, null, ACCESS_TOKEN);

        // then
        GcpAddress address1 = new GcpAddress(INSTANCE_1_PRIVATE_IP, INSTANCE_1_PUBLIC_IP);
        GcpAddress address2 = new GcpAddress(INSTANCE_2_PRIVATE_IP, INSTANCE_2_PUBLIC_IP);
        assertEquals(asList(address1, address2), result);
    }

    /**
     * Reponse recorded from the real Cloud Compute API call.
     */
    private static String instancesResponse() {
        return String.format("{\n"
                        + "  \"kind\": \"compute#instanceList\",\n"
                        + "  \"id\": \"projects/hazelcast-33/zones/us-east1-b/instances\",\n"
                        + "  \"items\": [\n"
                        + "    {\n"
                        + "      \"kind\": \"compute#instance\",\n"
                        + "      \"id\": \"*********\",\n"
                        + "      \"creationTimestamp\": \"2017-05-18T07:54:24.521-07:00\",\n"
                        + "      \"name\": \"hazelcast-3-8-1-1\",\n"
                        + "      \"description\": \"\",\n"
                        + "      \"tags\": {\n"
                        + "        \"items\": [\n"
                        + "          \"http-server\",\n"
                        + "          \"https-server\"\n"
                        + "        ],\n"
                        + "        \"fingerprint\": \"********\"\n"
                        + "      },\n"
                        + "      \"machineType\": \"https://www.googleapis.com/compute/v1/projects/hazelcast-33/zones/us-east1-b/machineTypes/n1-standard-1\",\n"
                        + "      \"status\": \"RUNNING\",\n"
                        + "      \"zone\": \"https://www.googleapis.com/compute/v1/projects/hazelcast-33/zones/us-east1-b\",\n"
                        + "      \"canIpForward\": false,\n"
                        + "      \"networkInterfaces\": [\n"
                        + "        {\n"
                        + "          \"kind\": \"compute#networkInterface\",\n"
                        + "          \"network\": \"https://www.googleapis.com/compute/v1/projects/hazelcast-33/global/networks/default\",\n"
                        + "          \"networkIP\": \"%s\",\n"
                        + "          \"name\": \"nic0\",\n"
                        + "          \"accessConfigs\": [\n"
                        + "            {\n"
                        + "              \"kind\": \"compute#accessConfig\",\n"
                        + "              \"type\": \"ONE_TO_ONE_NAT\",\n"
                        + "              \"name\": \"External NAT\",\n"
                        + "              \"natIP\": \"%s\",\n"
                        + "              \"networkTier\": \"STANDARD\"\n"
                        + "            }\n"
                        + "          ],\n"
                        + "          \"fingerprint\": \"*********\"\n"
                        + "        }\n"
                        + "      ],\n"
                        + "      \"disks\": [\n"
                        + "        {\n"
                        + "          \"kind\": \"compute#attachedDisk\",\n"
                        + "          \"type\": \"PERSISTENT\",\n"
                        + "          \"mode\": \"READ_WRITE\",\n"
                        + "          \"source\": \"https://www.googleapis.com/compute/v1/projects/hazelcast-33/zones/us-east1-b/disks/hazelcast-3-8-1-1\",\n"
                        + "          \"deviceName\": \"hazelcast-3-8-1-1\",\n"
                        + "          \"index\": 0,\n"
                        + "          \"boot\": true,\n"
                        + "          \"autoDelete\": true,\n"
                        + "          \"licenses\": [\n"
                        + "            \"https://www.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/licenses/ubuntu-1604-xenial\"\n"
                        + "          ],\n"
                        + "          \"interface\": \"SCSI\"\n"
                        + "        }\n"
                        + "      ],\n"
                        + "      \"metadata\": {\n"
                        + "        \"kind\": \"compute#metadata\",\n"
                        + "        \"fingerprint\": \"*******\"\n"
                        + "      },\n"
                        + "      \"serviceAccounts\": [\n"
                        + "        {\n"
                        + "          \"email\": \"*******@developer.gserviceaccount.com\",\n"
                        + "          \"scopes\": [\n"
                        + "            \"https://www.googleapis.com/auth/devstorage.read_only\",\n"
                        + "            \"https://www.googleapis.com/auth/logging.write\",\n"
                        + "            \"https://www.googleapis.com/auth/monitoring.write\",\n"
                        + "            \"https://www.googleapis.com/auth/servicecontrol\",\n"
                        + "            \"https://www.googleapis.com/auth/service.management.readonly\",\n"
                        + "            \"https://www.googleapis.com/auth/trace.append\"\n"
                        + "          ]\n"
                        + "        }\n"
                        + "      ],\n"
                        + "      \"selfLink\": \"https://www.googleapis.com/compute/v1/projects/hazelcast-33/zones/us-east1-b/instances/hazelcast-3-8-1-1\",\n"
                        + "      \"scheduling\": {\n"
                        + "        \"onHostMaintenance\": \"MIGRATE\",\n"
                        + "        \"automaticRestart\": true,\n"
                        + "        \"preemptible\": false\n"
                        + "      },\n"
                        + "      \"cpuPlatform\": \"Intel Haswell\",\n"
                        + "      \"labelFingerprint\": \"********\",\n"
                        + "      \"startRestricted\": false,\n"
                        + "      \"deletionProtection\": false\n"
                        + "    },\n"
                        + "    {\n"
                        + "      \"kind\": \"compute#instance\",\n"
                        + "      \"id\": \"**********\",\n"
                        + "      \"creationTimestamp\": \"2018-08-09T02:12:18.097-07:00\",\n"
                        + "      \"name\": \"rafal-test\",\n"
                        + "      \"description\": \"\",\n"
                        + "      \"tags\": {\n"
                        + "        \"items\": [\n"
                        + "          \"hazelcast\"\n"
                        + "        ],\n"
                        + "        \"fingerprint\": \"*******\"\n"
                        + "      },\n"
                        + "      \"machineType\": \"https://www.googleapis.com/compute/v1/projects/hazelcast-33/zones/us-east1-b/machineTypes/n1-standard-1\",\n"
                        + "      \"status\": \"RUNNING\",\n"
                        + "      \"zone\": \"https://www.googleapis.com/compute/v1/projects/hazelcast-33/zones/us-east1-b\",\n"
                        + "      \"canIpForward\": false,\n"
                        + "      \"networkInterfaces\": [\n"
                        + "        {\n"
                        + "          \"kind\": \"compute#networkInterface\",\n"
                        + "          \"network\": \"https://www.googleapis.com/compute/v1/projects/hazelcast-33/global/networks/default\",\n"
                        + "          \"networkIP\": \"%s\",\n"
                        + "          \"name\": \"nic0\",\n"
                        + "          \"accessConfigs\": [\n"
                        + "            {\n"
                        + "              \"kind\": \"compute#accessConfig\",\n"
                        + "              \"type\": \"ONE_TO_ONE_NAT\",\n"
                        + "              \"name\": \"External NAT\",\n"
                        + "              \"natIP\": \"%s\",\n"
                        + "              \"networkTier\": \"PREMIUM\"\n"
                        + "            }\n"
                        + "          ],\n"
                        + "          \"fingerprint\": \"********\"\n"
                        + "        }\n"
                        + "      ],\n"
                        + "      \"disks\": [\n"
                        + "        {\n"
                        + "          \"kind\": \"compute#attachedDisk\",\n"
                        + "          \"type\": \"PERSISTENT\",\n"
                        + "          \"mode\": \"READ_WRITE\",\n"
                        + "          \"source\": \"https://www.googleapis.com/compute/v1/projects/hazelcast-33/zones/us-east1-b/disks/rafal-test\",\n"
                        + "          \"deviceName\": \"rafal-test\",\n"
                        + "          \"index\": 0,\n"
                        + "          \"boot\": true,\n"
                        + "          \"autoDelete\": true,\n"
                        + "          \"licenses\": [\n"
                        + "            \"https://www.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/licenses/ubuntu-1604-xenial\"\n"
                        + "          ],\n"
                        + "          \"interface\": \"SCSI\",\n"
                        + "          \"guestOsFeatures\": [\n"
                        + "            {\n"
                        + "              \"type\": \"VIRTIO_SCSI_MULTIQUEUE\"\n"
                        + "            }\n"
                        + "          ]\n"
                        + "        }\n"
                        + "      ],\n"
                        + "      \"metadata\": {\n"
                        + "        \"kind\": \"compute#metadata\",\n"
                        + "        \"fingerprint\": \"********\"\n"
                        + "      },\n"
                        + "      \"serviceAccounts\": [\n"
                        + "        {\n"
                        + "          \"email\": \"*********@hazelcast-33.iam.gserviceaccount.com\",\n"
                        + "          \"scopes\": [\n"
                        + "            \"https://www.googleapis.com/auth/cloud-platform\"\n"
                        + "          ]\n"
                        + "        }\n"
                        + "      ],\n"
                        + "      \"selfLink\": \"https://www.googleapis.com/compute/v1/projects/hazelcast-33/zones/us-east1-b/instances/rafal-test\",\n"
                        + "      \"scheduling\": {\n"
                        + "        \"onHostMaintenance\": \"MIGRATE\",\n"
                        + "        \"automaticRestart\": true,\n"
                        + "        \"preemptible\": false\n"
                        + "      },\n"
                        + "      \"cpuPlatform\": \"Intel Haswell\",\n"
                        + "      \"labels\": {\n"
                        + "        \"application\": \"hazelcast\"\n"
                        + "      },\n"
                        + "      \"labelFingerprint\": \"*******\",\n"
                        + "      \"startRestricted\": false,\n"
                        + "      \"deletionProtection\": false\n"
                        + "    },\n"
                        + "    {\n"
                        + "      \"kind\": \"compute#instance\",\n"
                        + "      \"id\": \"***********\",\n"
                        + "      \"creationTimestamp\": \"2018-08-13T07:17:30.113-07:00\",\n"
                        + "      \"name\": \"rafal-test-2\",\n"
                        + "      \"description\": \"\",\n"
                        + "      \"tags\": {\n"
                        + "        \"fingerprint\": \"********\"\n"
                        + "      },\n"
                        + "      \"machineType\": \"https://www.googleapis.com/compute/v1/projects/hazelcast-33/zones/us-east1-b/machineTypes/n1-standard-1\",\n"
                        + "      \"status\": \"TERMINATED\",\n"
                        + "      \"zone\": \"https://www.googleapis.com/compute/v1/projects/hazelcast-33/zones/us-east1-b\",\n"
                        + "      \"canIpForward\": false,\n"
                        + "      \"networkInterfaces\": [\n"
                        + "        {\n"
                        + "          \"kind\": \"compute#networkInterface\",\n"
                        + "          \"network\": \"https://www.googleapis.com/compute/v1/projects/hazelcast-33/global/networks/default\",\n"
                        + "          \"networkIP\": \"%s\",\n"
                        + "          \"name\": \"nic0\",\n"
                        + "          \"accessConfigs\": [\n"
                        + "            {\n"
                        + "              \"kind\": \"compute#accessConfig\",\n"
                        + "              \"type\": \"ONE_TO_ONE_NAT\",\n"
                        + "              \"name\": \"External NAT\",\n"
                        + "              \"networkTier\": \"PREMIUM\"\n"
                        + "            }\n"
                        + "          ],\n"
                        + "          \"fingerprint\": \"*********\"\n"
                        + "        }\n"
                        + "      ],\n"
                        + "      \"disks\": [\n"
                        + "        {\n"
                        + "          \"kind\": \"compute#attachedDisk\",\n"
                        + "          \"type\": \"PERSISTENT\",\n"
                        + "          \"mode\": \"READ_WRITE\",\n"
                        + "          \"source\": \"https://www.googleapis.com/compute/v1/projects/hazelcast-33/zones/us-east1-b/disks/rafal-test-2\",\n"
                        + "          \"deviceName\": \"rafal-test-2\",\n"
                        + "          \"index\": 0,\n"
                        + "          \"boot\": true,\n"
                        + "          \"autoDelete\": true,\n"
                        + "          \"licenses\": [\n"
                        + "            \"https://www.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/licenses/ubuntu-1604-xenial\"\n"
                        + "          ],\n"
                        + "          \"interface\": \"SCSI\",\n"
                        + "          \"guestOsFeatures\": [\n"
                        + "            {\n"
                        + "              \"type\": \"VIRTIO_SCSI_MULTIQUEUE\"\n"
                        + "            }\n"
                        + "          ]\n"
                        + "        }\n"
                        + "      ],\n"
                        + "      \"metadata\": {\n"
                        + "        \"kind\": \"compute#metadata\",\n"
                        + "        \"fingerprint\": \"********\"\n"
                        + "      },\n"
                        + "      \"serviceAccounts\": [\n"
                        + "        {\n"
                        + "          \"email\": \"********@developer.gserviceaccount.com\",\n"
                        + "          \"scopes\": [\n"
                        + "            \"https://www.googleapis.com/auth/devstorage.read_only\",\n"
                        + "            \"https://www.googleapis.com/auth/logging.write\",\n"
                        + "            \"https://www.googleapis.com/auth/monitoring.write\",\n"
                        + "            \"https://www.googleapis.com/auth/servicecontrol\",\n"
                        + "            \"https://www.googleapis.com/auth/service.management.readonly\",\n"
                        + "            \"https://www.googleapis.com/auth/trace.append\"\n"
                        + "          ]\n"
                        + "        }\n"
                        + "      ],\n"
                        + "      \"selfLink\": \"https://www.googleapis.com/compute/v1/projects/hazelcast-33/zones/us-east1-b/instances/rafal-test-2\",\n"
                        + "      \"scheduling\": {\n"
                        + "        \"onHostMaintenance\": \"MIGRATE\",\n"
                        + "        \"automaticRestart\": true,\n"
                        + "        \"preemptible\": false\n"
                        + "      },\n"
                        + "      \"cpuPlatform\": \"Unknown CPU Platform\",\n"
                        + "      \"labelFingerprint\": \"*********\",\n"
                        + "      \"startRestricted\": false,\n"
                        + "      \"deletionProtection\": false\n"
                        + "    }\n"
                        + "  ],\n"
                        + "  \"selfLink\": \"https://www.googleapis.com/compute/v1/projects/hazelcast-33/zones/us-east1-b/instances\"\n"
                        + "}  ", INSTANCE_1_PRIVATE_IP, INSTANCE_1_PUBLIC_IP, INSTANCE_2_PRIVATE_IP, INSTANCE_2_PUBLIC_IP,
                INSTANCE_3_PRIVATE_IP);
    }

    String regionResponse(String project, String region) {
        return ("{\n"
                + "  \"zones\": [\n"
                + "    \"https://www.googleapis.com/compute/v1/projects/{PROJECT}/zones/{REGION}-a\",\n"
                + "    \"https://www.googleapis.com/compute/v1/projects/{PROJECT}/zones/{REGION}-b\",\n"
                + "    \"https://www.googleapis.com/compute/v1/projects/{PROJECT}/zones/{REGION}-c\"\n"
                + "  ]\n"
                + "}")
                .replace("{PROJECT}", project)
                .replace("{REGION}", region);
    }
}
