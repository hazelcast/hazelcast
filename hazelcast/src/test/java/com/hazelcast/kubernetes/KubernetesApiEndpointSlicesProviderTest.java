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

public class KubernetesApiEndpointSlicesProviderTest
        extends KubernetesApiProviderTest {

    public KubernetesApiEndpointSlicesProviderTest() {
        super(new KubernetesApiEndpointSlicesProvider());
    }

    @Override
    public String getEndpointsResponseWithServices() {
        //language=JSON
        return "{\n"
           + "  \"kind\": \"EndpointSliceList\",\n"
           + "  \"apiVersion\": \"discovery.k8s.io/v1\",\n"
           + "  \"items\": [\n"
           + "    {\n"
           + "      \"metadata\": {\n"
           + "        \"name\": \"hazelcast-0\",\n"
           + "        \"ownerReferences\": [\n"
           + "          {\n"
           + "            \"apiVersion\": \"v1\",\n"
           + "            \"kind\": \"Service\",\n"
           + "            \"name\": \"hazelcast-0\",\n"
           + "            \"controller\": true\n"
           + "          }\n"
           + "        ]\n"
           + "      },\n"
           + "      \"addressType\": \"IPv4\",\n"
           + "      \"endpoints\": [\n"
           + "        {\n"
           + "          \"addresses\": [\n"
           + "            \"192.168.0.25\"\n"
           + "          ],\n"
           + "          \"conditions\": {\n"
           + "            \"ready\": true\n"
           + "          },\n"
           + "          \"targetRef\": {\n"
           + "            \"kind\": \"Pod\",\n"
           + "            \"name\": \"hazelcast-0\"\n"
           + "          },\n"
           + "          \"nodeName\": \"nodeName-1\"\n"
           + "        }\n"
           + "      ],\n"
           + "      \"ports\": [\n"
           + "        {\n"
           + "          \"name\": \"5701\",\n"
           + "          \"protocol\": \"TCP\",\n"
           + "          \"port\": 5701\n"
           + "        }\n"
           + "      ]\n"
           + "    },\n"
           + "    {\n"
           + "      \"metadata\": {\n"
           + "        \"name\": \"service-1\",\n"
           + "        \"ownerReferences\": [\n"
           + "          {\n"
           + "            \"apiVersion\": \"v1\",\n"
           + "            \"kind\": \"Service\",\n"
           + "            \"name\": \"service-1\",\n"
           + "            \"controller\": true\n"
           + "          }\n"
           + "        ]\n"
           + "      },\n"
           + "      \"addressType\": \"IPv4\",\n"
           + "      \"endpoints\": [\n"
           + "        {\n"
           + "          \"addresses\": [\n"
           + "            \"172.17.0.5\"\n"
           + "          ],\n"
           + "          \"conditions\": {\n"
           + "            \"ready\": true\n"
           + "          },\n"
           + "          \"targetRef\": {\n"
           + "            \"kind\": \"Pod\",\n"
           + "            \"name\": \"hazelcast-1\"\n"
           + "          },\n"
           + "          \"nodeName\": \"nodeName-2\"\n"
           + "        }\n"
           + "      ],\n"
           + "      \"ports\": [\n"
           + "        {\n"
           + "          \"name\": \"5701\",\n"
           + "          \"protocol\": \"TCP\",\n"
           + "          \"port\": 5701\n"
           + "        }\n"
           + "      ]\n"
           + "    },\n"
           + "    {\n"
           + "      \"metadata\": {\n"
           + "        \"name\": \"my-release-hazelcast\",\n"
           + "        \"ownerReferences\": [\n"
           + "          {\n"
           + "            \"apiVersion\": \"v1\",\n"
           + "            \"kind\": \"Service\",\n"
           + "            \"name\": \"my-release-hazelcast\",\n"
           + "            \"controller\": true\n"
           + "          }\n"
           + "        ]\n"
           + "      },\n"
           + "      \"addressType\": \"IPv4\",\n"
           + "      \"endpoints\": [\n"
           + "        {\n"
           + "          \"addresses\": [\n"
           + "            \"192.168.0.25\"\n"
           + "          ],\n"
           + "          \"conditions\": {\n"
           + "            \"ready\": true\n"
           + "          },\n"
           + "          \"targetRef\": {\n"
           + "            \"kind\": \"Pod\",\n"
           + "            \"name\": \"hazelcast-0\"\n"
           + "          },\n"
           + "          \"nodeName\": \"node-name-1\"\n"
           + "        },\n"
           + "        {\n"
           + "          \"addresses\": [\n"
           + "            \"172.17.0.5\"\n"
           + "          ],\n"
           + "          \"conditions\": {\n"
           + "            \"ready\": true\n"
           + "          },\n"
           + "          \"targetRef\": {\n"
           + "            \"kind\": \"Pod\",\n"
           + "            \"name\": \"hazelcast-1\"\n"
           + "          },\n"
           + "          \"nodeName\": \"node-name-2\"\n"
           + "        }\n"
           + "      ],\n"
           + "      \"ports\": [\n"
           + "        {\n"
           + "          \"name\": \"5701\",\n"
           + "          \"protocol\": \"TCP\",\n"
           + "          \"port\": 5701\n"
           + "        }\n"
           + "      ]\n"
           + "    },\n"
           + "    {\n"
           + "      \"metadata\": {\n"
           + "        \"name\": \"kubernetes\",\n"
           + "        \"namespace\": \"default\",\n"
           + "        \"labels\": {\n"
           + "          \"kubernetes.io/service-name\": \"kubernetes\"\n"
           + "        }\n"
           + "      },\n"
           + "      \"addressType\": \"IPv4\",\n"
           + "      \"endpoints\": [\n"
           + "        {\n"
           + "          \"targetRef\": {\n"
           + "            \"name\": \"kubernetes\""
           + "          },\n"
           + "          \"addresses\": [\n"
           + "            \"34.122.156.52\"\n"
           + "          ],\n"
           + "          \"conditions\": {\n"
           + "            \"ready\": true\n"
           + "          }\n"
           + "        }\n"
           + "      ],\n"
           + "      \"ports\": [\n"
           + "        {\n"
           + "          \"name\": \"https\",\n"
           + "          \"protocol\": \"TCP\",\n"
           + "          \"port\": 443\n"
           + "        }\n"
           + "      ]\n"
           + "    }"
           + "  ]\n"
           + "}";
    }

    @Override
    public String getEndpointsResponse() {
        //language=JSON
        return "{\n"
               + "  \"kind\": \"EndpointSliceList\",\n"
               + "  \"apiVersion\": \"discovery.k8s.io/v1\",\n"
               + "  \"items\": [\n"
               + "    {\n"
               + "      \"metadata\": {\n"
               + "        \"name\": \"service-0\",\n"
               + "        \"ownerReferences\": [\n"
               + "          {\n"
               + "            \"apiVersion\": \"v1\",\n"
               + "            \"kind\": \"Service\",\n"
               + "            \"name\": \"service-0\",\n"
               + "            \"controller\": true\n"
               + "          }\n"
               + "        ]\n"
               + "      },\n"
               + "      \"addressType\": \"IPv4\",\n"
               + "      \"endpoints\": [\n"
               + "        {\n"
               + "          \"addresses\": [\n"
               + "            \"172.17.0.5\"\n"
               + "          ],\n"
               + "          \"conditions\": {\n"
               + "            \"ready\": true\n"
               + "          },\n"
               + "          \"targetRef\": {\n"
               + "            \"kind\": \"Pod\",\n"
               + "            \"name\": \"pod-0\"\n"
               + "          },\n"
               + "          \"nodeName\": \"nodeName-0\"\n"
               + "        },\n"
               + "        {\n"
               + "          \"addresses\": [\n"
               + "            \"192.168.0.25\"\n"
               + "          ],\n"
               + "          \"conditions\": {\n"
               + "            \"ready\": true\n"
               + "          },\n"
               + "          \"targetRef\": {\n"
               + "            \"kind\": \"Pod\",\n"
               + "            \"name\": \"pod-1\"\n"
               + "          },\n"
               + "          \"nodeName\": \"node-name-1\"\n"
               + "        }\n"
               + "      ],\n"
               + "      \"ports\": [\n"
               + "        {\n"
               + "          \"name\": \"5701\",\n"
               + "          \"protocol\": \"TCP\",\n"
               + "          \"port\": 5701\n"
               + "        }\n"
               + "      ]\n"
               + "    }\n"
               + "  ]\n"
               + "}";
    }

    @Override
    public String getEndpointsListResponse() {
        //language=JSON
        return "{\n"
               + "  \"kind\": \"EndpointSliceList\",\n"
               + "  \"apiVersion\": \"discovery.k8s.io/v1\",\n"
               + "  \"items\": [\n"
               + "    {\n"
               + "      \"metadata\": {\n"
               + "        \"name\": \"service-0\",\n"
               + "        \"ownerReferences\": [\n"
               + "          {\n"
               + "            \"apiVersion\": \"v1\",\n"
               + "            \"kind\": \"Service\",\n"
               + "            \"name\": \"service-0\",\n"
               + "            \"controller\": true\n"
               + "          }\n"
               + "        ]\n"
               + "      },\n"
               + "      \"addressType\": \"IPv4\",\n"
               + "      \"endpoints\": [\n"
               + "        {\n"
               + "          \"addresses\": [\n"
               + "            \"172.17.0.5\"\n"
               + "          ],\n"
               + "          \"conditions\": {\n"
               + "            \"ready\": true\n"
               + "          },\n"
               + "          \"targetRef\": {\n"
               + "            \"kind\": \"Pod\",\n"
               + "            \"name\": \"pod-0\"\n"
               + "          },\n"
               + "          \"nodeName\": \"nodeName-0\"\n"
               + "        },\n"
               + "        {\n"
               + "          \"addresses\": [\n"
               + "            \"192.168.0.25\"\n"
               + "          ],\n"
               + "          \"conditions\": {\n"
               + "            \"ready\": true\n"
               + "          },\n"
               + "          \"targetRef\": {\n"
               + "            \"kind\": \"Pod\",\n"
               + "            \"name\": \"pod-1\"\n"
               + "          },\n"
               + "          \"nodeName\": \"nodeName-1\"\n"
               + "        },\n"
               + "        {\n"
               + "          \"addresses\": [\n"
               + "            \"172.17.0.6\"\n"
               + "          ],\n"
               + "          \"conditions\": {\n"
               + "            \"ready\": false\n"
               + "          },\n"
               + "          \"targetRef\": {\n"
               + "            \"kind\": \"Pod\",\n"
               + "            \"name\": \"pod-2\"\n"
               + "          },\n"
               + "          \"nodeName\": \"node-name-2\"\n"
               + "        }\n"
               + "      ],\n"
               + "      \"ports\": [\n"
               + "        {\n"
               + "          \"name\": \"5701\",\n"
               + "          \"protocol\": \"TCP\",\n"
               + "          \"port\": 5701\n"
               + "        },\n"
               + "        {\n"
               + "          \"name\": \"hazelcast-service-port\",\n"
               + "          \"protocol\": \"TCP\",\n"
               + "          \"port\": 5702\n"
               + "        }\n"
               + "      ]\n"
               + "    }\n"
               + "  ]\n"
               + "}";
    }

    @Override
    public String getEndpointsUrlString() {
        return "%s/apis/discovery.k8s.io/v1/namespaces/%s/endpointslices";
    }

    @Override
    public String getEndpointsByNameUrlString() {
        return "%s/apis/discovery.k8s.io/v1/namespaces/%s/endpointslices?labelSelector=kubernetes.io/service-name=%s";
    }

    @Override
    public String getEndpointsByServiceLabelUrlString() {
        return "%s/apis/discovery.k8s.io/v1/namespaces/%s/endpointslices?%s";
    }
}
