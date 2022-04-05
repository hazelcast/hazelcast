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


public class KubernetesApiEndpointProviderTest
        extends KubernetesApiProviderTest {

    public KubernetesApiEndpointProviderTest() {
        super(new KubernetesApiEndpointProvider());
    }

    public String getEndpointsResponseWithServices() {
        //language=JSON
        return "{\n"
          + "  \"kind\": \"EndpointsList\",\n"
          + "  \"items\": [\n"
          + "    {\n"
          + "      \"metadata\": {\n"
          + "        \"name\": \"my-release-hazelcast\"\n"
          + "      },\n"
          + "      \"subsets\": [\n"
          + "        {\n"
          + "          \"addresses\": [\n"
          + "            {\n"
          + "              \"ip\": \"192.168.0.25\",\n"
          + "              \"nodeName\": \"node-name-1\"\n"
          + "            },\n"
          + "            {\n"
          + "              \"ip\": \"172.17.0.5\",\n"
          + "              \"nodeName\": \"node-name-2\"\n"
          + "            }\n"
          + "          ],\n"
          + "          \"ports\": [\n"
          + "            {\n"
          + "              \"port\": 5701\n"
          + "            }\n"
          + "          ]\n"
          + "        }\n"
          + "      ]\n"
          + "    },\n"
          + "    {\n"
          + "      \"metadata\": {\n"
          + "        \"name\": \"service-0\"\n"
          + "      },\n"
          + "      \"subsets\": [\n"
          + "        {\n"
          + "          \"addresses\": [\n"
          + "            {\n"
          + "              \"ip\": \"192.168.0.25\",\n"
          + "              \"nodeName\": \"node-name-1\"\n"
          + "            }\n"
          + "          ],\n"
          + "          \"ports\": [\n"
          + "            {\n"
          + "              \"port\": 5701\n"
          + "            }\n"
          + "          ]\n"
          + "        }\n"
          + "      ]\n"
          + "    },\n"
          + "    {\n"
          + "      \"metadata\": {\n"
          + "        \"name\": \"hazelcast-0\"\n"
          + "      },\n"
          + "      \"subsets\": [\n"
          + "        {\n"
          + "          \"addresses\": [\n"
          + "            {\n"
          + "              \"ip\": \"192.168.0.25\",\n"
          + "              \"nodeName\": \"node-name-1\",\n"
          + "              \"targetRef\" : {\n"
          + "                \"name\" : \"hazelcast-0\"\n"
          + "              }\n"
          + "            }\n"
          + "          ],\n"
          + "          \"ports\": [\n"
          + "            {\n"
          + "              \"port\": 5701\n"
          + "            }\n"
          + "          ]\n"
          + "        }\n"
          + "      ]\n"
          + "    },\n"
          + "    {\n"
          + "      \"metadata\": {\n"
          + "        \"name\": \"service-1\"\n"
          + "      },\n"
          + "      \"subsets\": [\n"
          + "        {\n"
          + "          \"addresses\": [\n"
          + "            {\n"
          + "              \"ip\": \"172.17.0.5\",\n"
          + "              \"nodeName\": \"node-name-2\"\n"
          + "            }\n"
          + "          ],\n"
          + "          \"ports\": [\n"
          + "            {\n"
          + "              \"port\": 5701\n"
          + "            }\n"
          + "          ]\n"
          + "        }\n"
          + "      ]\n"
          + "    }\n"
          + "  ]\n"
          + "}";
    }

    public String getEndpointsResponse() {
        //language=JSON
        return "{\n"
               + "  \"kind\": \"Endpoints\",\n"
               + "  \"subsets\": [\n"
               + "    {\n"
               + "      \"addresses\": [\n"
               + "        {\n"
               + "          \"ip\": \"192.168.0.25\"\n"
               + "        },\n"
               + "        {\n"
               + "          \"ip\": \"172.17.0.5\"\n"
               + "        }\n"
               + "      ],\n"
               + "      \"ports\": [\n"
               + "        {\n"
               + "          \"name\": \"5701\",\n"
               + "          \"port\": 5701,\n"
               + "          \"protocol\": \"TCP\"\n"
               + "        }\n"
               + "      ]\n"
               + "    }\n"
               + "  ]\n"
               + "}";
    }

    public String getEndpointsListResponse() {
        //language=JSON
        return "{\n"
               + "  \"kind\": \"EndpointsList\",\n"
               + "  \"items\": [\n"
               + "    {\n"
               + "      \"subsets\": [\n"
               + "        {\n"
               + "          \"addresses\": [\n"
               + "            {\n"
               + "              \"ip\": \"172.17.0.5\"\n"
               + "            },\n"
               + "            {\n"
               + "              \"ip\": \"192.168.0.25\"\n"
               + "            }\n"
               + "          ],\n"
               + "          \"notReadyAddresses\": [\n"
               + "            {\n"
               + "              \"ip\": \"172.17.0.6\"\n"
               + "            }\n"
               + "          ],\n"
               + "          \"ports\": [\n"
               + "            {\n"
               + "              \"port\": 5701\n"
               + "            },\n"
               + "            {\n"
               + "              \"name\": \"hazelcast-service-port\",\n"
               + "              \"protocol\": \"TCP\",\n"
               + "              \"port\": 5702\n"
               + "            }\n"
               + "          ]\n"
               + "        }\n"
               + "      ]\n"
               + "    }\n"
               + "  ]\n"
               + "}";
    }

    public String getEndpointsUrlString() {
        return "%s/api/v1/namespaces/%s/endpoints";
    }

    public String getEndpointsByNameUrlString() {
        return "%s/api/v1/namespaces/%s/endpoints/%s";
    }

    public String getEndpointsByServiceLabelUrlString() {
        return "%s/api/v1/namespaces/%s/endpoints?%s";
    }
}
