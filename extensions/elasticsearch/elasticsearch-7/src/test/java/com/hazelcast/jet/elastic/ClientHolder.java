/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.elastic;

import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.elasticsearch.client.RestClient;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.starter.ReflectionUtils.getFieldValueReflectively;
import static java.util.Collections.synchronizedList;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * Holder for created Elastic clients during tests
 */
final class ClientHolder implements Serializable {
    static List<RestClient> elasticClients = synchronizedList(new ArrayList<>());

    private ClientHolder() {
    }

    static void assertAllClientsNotRunning() {
        assertTrueEventually(() -> {
            for (RestClient client : elasticClients) {
                CloseableHttpAsyncClient httpClient = getFieldValueReflectively(client, "client");
                assertThat(httpClient.isRunning()).isFalse();
            }
        });
    }
}
