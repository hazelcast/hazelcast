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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.elastic.impl.Shard.Prirep;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RequestOptions.Builder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.slice.SliceBuilder;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.assertj.core.util.Lists.newArrayList;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class ElasticSourcePTest {

    public static final String HIT_SOURCE = "{\"name\": \"Frantisek\"}";
    public static final String HIT_SOURCE2 = "{\"name\": \"Vladimir\"}";
    public static final String SCROLL_ID = "random-scroll-id";

    private static final String KEEP_ALIVE = "42m";

    private ElasticSourceP<String> processor;
    private SerializableRestClient mockClient;
    private SearchResponse response;

    @Before
    public void setUp() throws Exception {
        mockClient = SerializableRestClient.instanceHolder = mock(SerializableRestClient.class, RETURNS_DEEP_STUBS);
        // Mocks returning mocks is not generally recommended, but the setup of empty SearchResponse is even uglier
        // See org.elasticsearch.action.search.SearchResponse#empty
        response = mock(SearchResponse.class);
        when(response.getScrollId()).thenReturn(SCROLL_ID);
        when(mockClient.search(any(SearchRequest.class), any(RequestOptions.class))).thenReturn(response);
    }

    private TestSupport runProcessor() throws Exception {
        return runProcessor(request -> RequestOptions.DEFAULT, emptyList(), false, false);
    }

    private TestSupport runProcessor(FunctionEx<ActionRequest, RequestOptions> optionsFn) throws Exception {
        return runProcessor(optionsFn, emptyList(), false, false);
    }

    private TestSupport runProcessorWithCoLocation(List<Shard> shards) throws Exception {
        return runProcessor(request -> RequestOptions.DEFAULT, shards, false, true);
    }

    private TestSupport runProcessor(FunctionEx<ActionRequest, RequestOptions> optionsFn, List<Shard> shards,
                                     boolean slicing, boolean coLocatedReading)
            throws Exception {

        RestHighLevelClient client = mockClient;
        ElasticSourceConfiguration<String> configuration = new ElasticSourceConfiguration<String>(
                () -> client,
                () -> new SearchRequest("*"),
                optionsFn,
                SearchHit::getSourceAsString,
                slicing,
                coLocatedReading,
                KEEP_ALIVE
        );

        // This constructor calls the client so it has to be called after specific mock setup in each test method
        // rather than in setUp()
        processor = new ElasticSourceP<>(configuration, shards);

        return TestSupport.verifyProcessor(() -> processor)
                .disableSnapshots();
    }

    @Test
    public void when_runProcessor_then_executeSearchRequestWithScroll() throws Exception {
        when(response.getHits()).thenReturn(new SearchHits(new SearchHit[]{}, 0, Float.NaN));

        TestSupport support = runProcessor();

        support.expectOutput(emptyList());

        ArgumentCaptor<SearchRequest> captor = forClass(SearchRequest.class);
        verify(mockClient).search(captor.capture(), any(RequestOptions.class));

        SearchRequest request = captor.getValue();
        assertThat(request.scroll().keepAlive().getStringRep()).isEqualTo(KEEP_ALIVE);
    }

    @Test
    public void when_runProcessorWithOptionsFn_then_shouldUseOptionsFnForSearchRequest() throws Exception {
        when(response.getHits()).thenReturn(new SearchHits(new SearchHit[]{}, 0, Float.NaN));

        // get different instance than default
        TestSupport testSupport = runProcessor(request -> {
            Builder builder = RequestOptions.DEFAULT.toBuilder();
            builder.addHeader("TestHeader", "value");
            return builder.build();
        });

        testSupport.expectOutput(emptyList());

        ArgumentCaptor<RequestOptions> captor = forClass(RequestOptions.class);
        verify(mockClient).search(any(), captor.capture());

        RequestOptions capturedOptions = captor.getValue();
        assertThat(capturedOptions.getHeaders())
                .extracting(h -> tuple(h.getName(), h.getValue()))
                .containsExactly(tuple("TestHeader", "value"));
    }

    @Test
    public void given_singleHit_when_runProcessor_then_produceSingleHit() throws Exception {
        SearchHit hit = new SearchHit(0, "id-0", new Text("ignored"), emptyMap());
        hit.sourceRef(new BytesArray(HIT_SOURCE));
        when(response.getHits()).thenReturn(new SearchHits(new SearchHit[]{hit}, 1, Float.NaN));

        SearchResponse response2 = mock(SearchResponse.class);
        when(response2.getHits()).thenReturn(new SearchHits(new SearchHit[]{}, 1, Float.NaN));
        when(mockClient.scroll(any(), any())).thenReturn(response2);

        TestSupport testSupport = runProcessor();

        testSupport.expectOutput(newArrayList(HIT_SOURCE));
    }

    @Test
    public void givenMultipleResults_when_runProcessor_then_useScrollIdInFollowupScrollRequest() throws Exception {
        SearchHit hit = new SearchHit(0, "id-0", new Text("ignored"), emptyMap());
        hit.sourceRef(new BytesArray(HIT_SOURCE));
        when(response.getHits()).thenReturn(new SearchHits(new SearchHit[]{hit}, 3, Float.NaN));

        SearchResponse response2 = mock(SearchResponse.class);
        SearchHit hit2 = new SearchHit(1, "id-1", new Text("ignored"), emptyMap());
        hit2.sourceRef(new BytesArray(HIT_SOURCE2));
        when(response2.getHits()).thenReturn(new SearchHits(new SearchHit[]{hit2}, 3, Float.NaN));

        SearchResponse response3 = mock(SearchResponse.class);
        when(response3.getHits()).thenReturn(new SearchHits(new SearchHit[]{}, 3, Float.NaN));
        when(mockClient.scroll(any(), any())).thenReturn(response2, response3);

        TestSupport testSupport = runProcessor();

        testSupport.expectOutput(newArrayList(HIT_SOURCE, HIT_SOURCE2));

        ArgumentCaptor<SearchScrollRequest> captor = forClass(SearchScrollRequest.class);

        verify(mockClient, times(2)).scroll(captor.capture(), any());
        SearchScrollRequest request = captor.getValue();
        assertThat(request.scrollId()).isEqualTo(SCROLL_ID);
        assertThat(request.scroll().keepAlive().getStringRep()).isEqualTo(KEEP_ALIVE);
    }

    @Test
    public void when_runProcessorWithOptionsFn_then_shouldUseOptionsFnForScrollRequest() throws Exception {
        SearchHit hit = new SearchHit(0, "id-0", new Text("ignored"), emptyMap());
        hit.sourceRef(new BytesArray(HIT_SOURCE));
        when(response.getHits()).thenReturn(new SearchHits(new SearchHit[]{hit}, 1, Float.NaN));

        SearchResponse response2 = mock(SearchResponse.class);
        when(response2.getHits()).thenReturn(new SearchHits(new SearchHit[]{}, 1, Float.NaN));
        when(mockClient.scroll(any(), any())).thenReturn(response2);

        // get different instance than default
        TestSupport testSupport = runProcessor(request -> {
            Builder builder = RequestOptions.DEFAULT.toBuilder();
            builder.addHeader("TestHeader", "value");
            return builder.build();
        });

        testSupport.expectOutput(newArrayList(HIT_SOURCE));

        ArgumentCaptor<RequestOptions> captor = forClass(RequestOptions.class);
        verify(mockClient).scroll(any(), captor.capture());

        RequestOptions capturedOptions = captor.getValue();
        assertThat(capturedOptions.getHeaders())
                .extracting(h -> tuple(h.getName(), h.getValue()))
                .containsExactly(tuple("TestHeader", "value"));
    }

    @Test
    public void when_runProcessorWithCoLocation_then_useLocalNodeOnly() throws Exception {
        RestClient lowClient = mock(RestClient.class);
        when(mockClient.getLowLevelClient()).thenReturn(lowClient);
        when(response.getHits()).thenReturn(new SearchHits(new SearchHit[]{}, 0, Float.NaN));

        TestSupport testSupport = runProcessorWithCoLocation(newArrayList(
                new Shard("my-index", 0, Prirep.p, 42, "STARTED", "10.0.0.1", "10.0.0.1:9200", "es1")
        ));
        testSupport.expectOutput(emptyList());

        ArgumentCaptor<Collection<Node>> nodesCaptor = ArgumentCaptor.forClass(Collection.class);

        verify(lowClient).setNodes(nodesCaptor.capture());

        Collection<Node> nodes = nodesCaptor.getValue();
        assertThat(nodes).hasSize(1);

        Node node = nodes.iterator().next();
        assertThat(node.getHost().toHostString()).isEqualTo("10.0.0.1:9200");
    }

    @Test
    public void when_runProcessorWithCoLocation_thenSearchShardsWithPreference() throws Exception {
        when(response.getHits()).thenReturn(new SearchHits(new SearchHit[]{}, 0, Float.NaN));

        TestSupport processor = runProcessorWithCoLocation(newArrayList(
                new Shard("my-index", 0, Prirep.p, 42, "STARTED", "10.0.0.1", "10.0.0.1:9200", "es1"),
                new Shard("my-index", 1, Prirep.p, 42, "STARTED", "10.0.0.1", "10.0.0.1:9200", "es1"),
                new Shard("my-index", 2, Prirep.p, 42, "STARTED", "10.0.0.1", "10.0.0.1:9200", "es1")
        ));
        processor.expectOutput(emptyList());

        ArgumentCaptor<SearchRequest> captor = forClass(SearchRequest.class);
        verify(mockClient).search(captor.capture(), any(RequestOptions.class));

        SearchRequest request = captor.getValue();
        assertThat(request.preference()).isEqualTo("_shards:0,1,2|_only_local");
    }

    @Test
    public void when_runProcessorWithParallelism_thenUseSlicingBasedOnGlobalValues() throws Exception {
        when(response.getHits()).thenReturn(new SearchHits(new SearchHit[]{}, 0, Float.NaN));

        TestSupport testSupport = runProcessor((r) -> RequestOptions.DEFAULT, emptyList(), true, false);
        testSupport.localProcessorIndex(1);
        testSupport.localParallelism(2);
        testSupport.globalProcessorIndex(4);
        testSupport.totalParallelism(6);
        testSupport.expectOutput(emptyList());

        ArgumentCaptor<SearchRequest> captor = forClass(SearchRequest.class);
        verify(mockClient).search(captor.capture(), any(RequestOptions.class));

        SearchRequest request = captor.getValue();
        SliceBuilder slice = request.source().slice();

        // Slicing across all, should use global index / total parallelism
        assertThat(slice.getId()).isEqualTo(4);
        assertThat(slice.getMax()).isEqualTo(6);
    }

    @Test
    public void when_runProcessorWithCoLocationAndSlicing_thenUseSlicingBasedOnLocalValues() throws Exception {
        when(response.getHits()).thenReturn(new SearchHits(new SearchHit[]{}, 0, Float.NaN));

        TestSupport testSupport = runProcessor((r) -> RequestOptions.DEFAULT,
                newArrayList(
                        new Shard("my-index", 0, Prirep.p, 42, "STARTED", "10.0.0.1", "10.0.0.1:9200", "es1"),
                        new Shard("my-index", 1, Prirep.p, 42, "STARTED", "10.0.0.1", "10.0.0.1:9200", "es1"),
                        new Shard("my-index", 2, Prirep.p, 42, "STARTED", "10.0.0.1", "10.0.0.1:9200", "es1")
                ),
                true, true);
        testSupport.localProcessorIndex(1);
        testSupport.localParallelism(2);
        testSupport.globalProcessorIndex(4);
        testSupport.totalParallelism(6);
        testSupport.expectOutput(emptyList());

        ArgumentCaptor<SearchRequest> captor = forClass(SearchRequest.class);
        verify(mockClient).search(captor.capture(), any(RequestOptions.class));

        SearchRequest request = captor.getValue();
        SliceBuilder slice = request.source().slice();

        // Slicing across single node, should use local values
        assertThat(slice.getId()).isEqualTo(1);
        assertThat(slice.getMax()).isEqualTo(2);
    }

    /*
     * Need to pass a Serializable Supplier into
     * ElasticSourceBuilder.clientFn(...)
     * which returns a mock, so the mock itself must be serializable.
     *
     * Can't use Mockito's withSettings().serializable() because some of the setup (SearchResponse) is not Serializable
     */
    static class SerializableRestClient extends RestHighLevelClient implements Serializable {

        static SerializableRestClient instanceHolder;

        SerializableRestClient(RestClientBuilder restClientBuilder) {
            super(restClientBuilder);
        }

    }
}
