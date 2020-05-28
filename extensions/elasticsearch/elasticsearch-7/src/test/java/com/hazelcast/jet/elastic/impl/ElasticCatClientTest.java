/*
 * Copyright 2020 Hazelcast Inc.
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

package com.hazelcast.jet.elastic.impl;

import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ElasticCatClientTest {

    @Mock
    private RestClient restClient;

    @Test
    public void shards() throws IOException {
        ElasticCatClient catClient = new ElasticCatClient(restClient);

        Response nodesResponse = response("es2node_nodes.json");
        Response shardsResponse = response("es2node_shards.json");
        when(restClient.performRequest(any()))
                .thenReturn(nodesResponse, shardsResponse);

        List<Shard> shards = catClient.shards("my-index");
        assertThat(shards).extracting(Shard::getHttpAddress)
                          .containsOnly("127.0.0.1:9200", "127.0.0.1:9201");
    }

    private Response response(String json) throws IOException {

        Response response = mock(Response.class, RETURNS_DEEP_STUBS);
        when(response.getEntity().getContent())
                .thenReturn(new FileInputStream("src/test/resources/mock_es_responses/" + json));

        return response;
    }
}
