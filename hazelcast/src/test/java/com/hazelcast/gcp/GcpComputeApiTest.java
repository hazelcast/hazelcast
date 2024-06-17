/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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
        return String.format("""
                        {
                          "kind": "compute#instanceList",
                          "id": "projects/hazelcast-33/zones/us-east1-b/instances",
                          "items": [
                            {
                              "kind": "compute#instance",
                              "id": "*********",
                              "creationTimestamp": "2017-05-18T07:54:24.521-07:00",
                              "name": "hazelcast-3-8-1-1",
                              "description": "",
                              "tags": {
                                "items": [
                                  "http-server",
                                  "https-server"
                                ],
                                "fingerprint": "********"
                              },
                              "machineType": "https://www.googleapis.com/compute/v1/projects/hazelcast-33/zones/us-east1-b/machineTypes/n1-standard-1",
                              "status": "RUNNING",
                              "zone": "https://www.googleapis.com/compute/v1/projects/hazelcast-33/zones/us-east1-b",
                              "canIpForward": false,
                              "networkInterfaces": [
                                {
                                  "kind": "compute#networkInterface",
                                  "network": "https://www.googleapis.com/compute/v1/projects/hazelcast-33/global/networks/default",
                                  "networkIP": "%s",
                                  "name": "nic0",
                                  "accessConfigs": [
                                    {
                                      "kind": "compute#accessConfig",
                                      "type": "ONE_TO_ONE_NAT",
                                      "name": "External NAT",
                                      "natIP": "%s",
                                      "networkTier": "STANDARD"
                                    }
                                  ],
                                  "fingerprint": "*********"
                                }
                              ],
                              "disks": [
                                {
                                  "kind": "compute#attachedDisk",
                                  "type": "PERSISTENT",
                                  "mode": "READ_WRITE",
                                  "source": "https://www.googleapis.com/compute/v1/projects/hazelcast-33/zones/us-east1-b/disks/hazelcast-3-8-1-1",
                                  "deviceName": "hazelcast-3-8-1-1",
                                  "index": 0,
                                  "boot": true,
                                  "autoDelete": true,
                                  "licenses": [
                                    "https://www.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/licenses/ubuntu-1604-xenial"
                                  ],
                                  "interface": "SCSI"
                                }
                              ],
                              "metadata": {
                                "kind": "compute#metadata",
                                "fingerprint": "*******"
                              },
                              "serviceAccounts": [
                                {
                                  "email": "*******@developer.gserviceaccount.com",
                                  "scopes": [
                                    "https://www.googleapis.com/auth/devstorage.read_only",
                                    "https://www.googleapis.com/auth/logging.write",
                                    "https://www.googleapis.com/auth/monitoring.write",
                                    "https://www.googleapis.com/auth/servicecontrol",
                                    "https://www.googleapis.com/auth/service.management.readonly",
                                    "https://www.googleapis.com/auth/trace.append"
                                  ]
                                }
                              ],
                              "selfLink": "https://www.googleapis.com/compute/v1/projects/hazelcast-33/zones/us-east1-b/instances/hazelcast-3-8-1-1",
                              "scheduling": {
                                "onHostMaintenance": "MIGRATE",
                                "automaticRestart": true,
                                "preemptible": false
                              },
                              "cpuPlatform": "Intel Haswell",
                              "labelFingerprint": "********",
                              "startRestricted": false,
                              "deletionProtection": false
                            },
                            {
                              "kind": "compute#instance",
                              "id": "**********",
                              "creationTimestamp": "2018-08-09T02:12:18.097-07:00",
                              "name": "rafal-test",
                              "description": "",
                              "tags": {
                                "items": [
                                  "hazelcast"
                                ],
                                "fingerprint": "*******"
                              },
                              "machineType": "https://www.googleapis.com/compute/v1/projects/hazelcast-33/zones/us-east1-b/machineTypes/n1-standard-1",
                              "status": "RUNNING",
                              "zone": "https://www.googleapis.com/compute/v1/projects/hazelcast-33/zones/us-east1-b",
                              "canIpForward": false,
                              "networkInterfaces": [
                                {
                                  "kind": "compute#networkInterface",
                                  "network": "https://www.googleapis.com/compute/v1/projects/hazelcast-33/global/networks/default",
                                  "networkIP": "%s",
                                  "name": "nic0",
                                  "accessConfigs": [
                                    {
                                      "kind": "compute#accessConfig",
                                      "type": "ONE_TO_ONE_NAT",
                                      "name": "External NAT",
                                      "natIP": "%s",
                                      "networkTier": "PREMIUM"
                                    }
                                  ],
                                  "fingerprint": "********"
                                }
                              ],
                              "disks": [
                                {
                                  "kind": "compute#attachedDisk",
                                  "type": "PERSISTENT",
                                  "mode": "READ_WRITE",
                                  "source": "https://www.googleapis.com/compute/v1/projects/hazelcast-33/zones/us-east1-b/disks/rafal-test",
                                  "deviceName": "rafal-test",
                                  "index": 0,
                                  "boot": true,
                                  "autoDelete": true,
                                  "licenses": [
                                    "https://www.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/licenses/ubuntu-1604-xenial"
                                  ],
                                  "interface": "SCSI",
                                  "guestOsFeatures": [
                                    {
                                      "type": "VIRTIO_SCSI_MULTIQUEUE"
                                    }
                                  ]
                                }
                              ],
                              "metadata": {
                                "kind": "compute#metadata",
                                "fingerprint": "********"
                              },
                              "serviceAccounts": [
                                {
                                  "email": "*********@hazelcast-33.iam.gserviceaccount.com",
                                  "scopes": [
                                    "https://www.googleapis.com/auth/cloud-platform"
                                  ]
                                }
                              ],
                              "selfLink": "https://www.googleapis.com/compute/v1/projects/hazelcast-33/zones/us-east1-b/instances/rafal-test",
                              "scheduling": {
                                "onHostMaintenance": "MIGRATE",
                                "automaticRestart": true,
                                "preemptible": false
                              },
                              "cpuPlatform": "Intel Haswell",
                              "labels": {
                                "application": "hazelcast"
                              },
                              "labelFingerprint": "*******",
                              "startRestricted": false,
                              "deletionProtection": false
                            },
                            {
                              "kind": "compute#instance",
                              "id": "***********",
                              "creationTimestamp": "2018-08-13T07:17:30.113-07:00",
                              "name": "rafal-test-2",
                              "description": "",
                              "tags": {
                                "fingerprint": "********"
                              },
                              "machineType": "https://www.googleapis.com/compute/v1/projects/hazelcast-33/zones/us-east1-b/machineTypes/n1-standard-1",
                              "status": "TERMINATED",
                              "zone": "https://www.googleapis.com/compute/v1/projects/hazelcast-33/zones/us-east1-b",
                              "canIpForward": false,
                              "networkInterfaces": [
                                {
                                  "kind": "compute#networkInterface",
                                  "network": "https://www.googleapis.com/compute/v1/projects/hazelcast-33/global/networks/default",
                                  "networkIP": "%s",
                                  "name": "nic0",
                                  "accessConfigs": [
                                    {
                                      "kind": "compute#accessConfig",
                                      "type": "ONE_TO_ONE_NAT",
                                      "name": "External NAT",
                                      "networkTier": "PREMIUM"
                                    }
                                  ],
                                  "fingerprint": "*********"
                                }
                              ],
                              "disks": [
                                {
                                  "kind": "compute#attachedDisk",
                                  "type": "PERSISTENT",
                                  "mode": "READ_WRITE",
                                  "source": "https://www.googleapis.com/compute/v1/projects/hazelcast-33/zones/us-east1-b/disks/rafal-test-2",
                                  "deviceName": "rafal-test-2",
                                  "index": 0,
                                  "boot": true,
                                  "autoDelete": true,
                                  "licenses": [
                                    "https://www.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/licenses/ubuntu-1604-xenial"
                                  ],
                                  "interface": "SCSI",
                                  "guestOsFeatures": [
                                    {
                                      "type": "VIRTIO_SCSI_MULTIQUEUE"
                                    }
                                  ]
                                }
                              ],
                              "metadata": {
                                "kind": "compute#metadata",
                                "fingerprint": "********"
                              },
                              "serviceAccounts": [
                                {
                                  "email": "********@developer.gserviceaccount.com",
                                  "scopes": [
                                    "https://www.googleapis.com/auth/devstorage.read_only",
                                    "https://www.googleapis.com/auth/logging.write",
                                    "https://www.googleapis.com/auth/monitoring.write",
                                    "https://www.googleapis.com/auth/servicecontrol",
                                    "https://www.googleapis.com/auth/service.management.readonly",
                                    "https://www.googleapis.com/auth/trace.append"
                                  ]
                                }
                              ],
                              "selfLink": "https://www.googleapis.com/compute/v1/projects/hazelcast-33/zones/us-east1-b/instances/rafal-test-2",
                              "scheduling": {
                                "onHostMaintenance": "MIGRATE",
                                "automaticRestart": true,
                                "preemptible": false
                              },
                              "cpuPlatform": "Unknown CPU Platform",
                              "labelFingerprint": "*********",
                              "startRestricted": false,
                              "deletionProtection": false
                            }
                          ],
                          "selfLink": "https://www.googleapis.com/compute/v1/projects/hazelcast-33/zones/us-east1-b/instances"
                        } \s""", INSTANCE_1_PRIVATE_IP, INSTANCE_1_PUBLIC_IP, INSTANCE_2_PRIVATE_IP, INSTANCE_2_PUBLIC_IP,
                INSTANCE_3_PRIVATE_IP);
    }

    String regionResponse(String project, String region) {
        return ("""
                {
                  "zones": [
                    "https://www.googleapis.com/compute/v1/projects/{PROJECT}/zones/{REGION}-a",
                    "https://www.googleapis.com/compute/v1/projects/{PROJECT}/zones/{REGION}-b",
                    "https://www.googleapis.com/compute/v1/projects/{PROJECT}/zones/{REGION}-c"
                  ]
                }""")
                .replace("{PROJECT}", project)
                .replace("{REGION}", region);
    }
}
