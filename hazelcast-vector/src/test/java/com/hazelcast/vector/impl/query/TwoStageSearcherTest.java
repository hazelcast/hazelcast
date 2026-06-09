/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.impl.query;

import com.hazelcast.cluster.Address;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.logging.ILogger;
import com.hazelcast.vector.SearchOptions;
import com.hazelcast.vector.VectorValues;
import com.hazelcast.vector.impl.ops.SearchMemberOperation;
import com.hazelcast.vector.impl.ops.SearchMemberResult;
import com.hazelcast.vector.impl.ops.SearchOperation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.UnknownHostException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.vector.impl.VectorTestUtils.heapData;
import static com.hazelcast.vector.impl.VectorTestUtils.srData;
import static com.hazelcast.vector.impl.VectorTestUtils.srs;
import static com.hazelcast.vector.impl.VectorTestUtils.toMap;
import static com.hazelcast.vector.impl.VectorTestUtils.vec;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TwoStageSearcherTest {
    @Mock
    IPartitionService partitionService;
    @Mock
    TwoStageSearcher.OperationInvoker operationService;
    @Mock
    ILogger logger;


    @InjectMocks
    TwoStageSearcher searcher;

    final String name = "vectorCollection";
    final VectorValues vectors = vec(1f);
    final SearchOptions options = SearchOptions.of(1, false, false);
    final Address member1 = new Address("localhost", 5701);
    final Address member2 = new Address("localhost", 5702);

    TwoStageSearcherTest() throws UnknownHostException {
    }

    @Test
    void shouldInvokeSearchOnSingleMember() throws ExecutionException, InterruptedException {
        final int partitionCount = 1;

        when(partitionService.getPartitionCount()).thenReturn(partitionCount);
        when(partitionService.getMemberPartitionsMap()).thenReturn(Map.of(member1, List.of(0)));
        when(operationService.invokeOnTargetAsync(any(SearchMemberOperation.class), eq(member1)))
                .thenReturn(CompletableFuture.completedFuture(new SearchMemberResult(srs(srData("result", 0.5f)),
                            new PartitionIdSet(partitionCount, 0))));

        var results = searcher.search(name, vectors, options).get();

        assertThat(results.size()).isEqualTo(1);
        assertThat(toMap(results)).containsOnlyKeys(heapData("result"));

        verify(operationService, never().description("Should not retry partitions"))
                .invokeOnPartitionAsync(any(), anyInt());
    }

    @Test
    void shouldInvokeSearchOnTwoMembers() throws ExecutionException, InterruptedException {
        final int partitionCount = 2;
        when(partitionService.getPartitionCount()).thenReturn(partitionCount);
        when(partitionService.getMemberPartitionsMap()).thenReturn(Map.of(member1, List.of(0), member2, List.of(1)));
        when(operationService.invokeOnTargetAsync(any(SearchMemberOperation.class), eq(member1)))
                .thenReturn(CompletableFuture.completedFuture(new SearchMemberResult(srs(srData("result", 0.5f)),
                        new PartitionIdSet(partitionCount, 0))));
        when(operationService.invokeOnTargetAsync(any(SearchMemberOperation.class), eq(member2)))
                .thenReturn(CompletableFuture.completedFuture(new SearchMemberResult(srs(srData("worse result", 0.4f)),
                        new PartitionIdSet(partitionCount, 1))));

        var results = searcher.search(name, vectors, options).get();
        assertThat(results.size()).isEqualTo(1);
        assertThat(toMap(results)).containsOnlyKeys(heapData("result"));

        verify(operationService, never().description("Should not retry partitions"))
                .invokeOnPartitionAsync(any(), anyInt());
    }


    @Test
    void shouldRetryMissingPartitionAndReturnResultIfBetter() throws ExecutionException, InterruptedException {
        final int partitionCount = 2;
        final int missingPartition = 0;
        when(partitionService.getPartitionCount()).thenReturn(partitionCount);
        when(partitionService.getMemberPartitionsMap()).thenReturn(Map.of(member1, List.of(0, 1)));
        when(operationService.invokeOnTargetAsync(any(SearchMemberOperation.class), eq(member1)))
                .thenReturn(CompletableFuture.completedFuture(new SearchMemberResult(srs(srData("result", 0.5f)),
                        new PartitionIdSet(partitionCount, 1))));
        when(operationService.invokeOnPartitionAsync(any(SearchOperation.class), eq(missingPartition)))
                .thenReturn(CompletableFuture.completedFuture(srs(srData("better result", 0.6f))));

        var results = searcher.search(name, vectors, options).get();

        assertThat(results.size()).isEqualTo(1);
        assertThat(toMap(results)).containsOnlyKeys(heapData("better result"));

        verify(operationService, times(1).description("Should retry partition"))
                .invokeOnPartitionAsync(any(), eq(missingPartition));
    }

    @Test
    void shouldRetryMissingPartitionAndNotReturnResultIfWorse() throws ExecutionException, InterruptedException {
        final int partitionCount = 2;
        final int missingPartition = 0;
        when(partitionService.getPartitionCount()).thenReturn(partitionCount);
        when(partitionService.getMemberPartitionsMap()).thenReturn(Map.of(member1, List.of(0, 1)));
        when(operationService.invokeOnTargetAsync(any(SearchMemberOperation.class), eq(member1)))
                .thenReturn(CompletableFuture.completedFuture(new SearchMemberResult(srs(srData("result", 0.5f)),
                        new PartitionIdSet(partitionCount, 1))));
        when(operationService.invokeOnPartitionAsync(any(SearchOperation.class), eq(missingPartition)))
                .thenReturn(CompletableFuture.completedFuture(srs(srData("worse result", 0.4f))));

        var results = searcher.search(name, vectors, options).get();

        assertThat(results.size()).isEqualTo(1);
        assertThat(toMap(results)).containsOnlyKeys(heapData("result"));

        verify(operationService, times(1).description("Should retry partition"))
                .invokeOnPartitionAsync(any(), eq(missingPartition));
    }

    @Test
    void shouldRetryFailedMemberPartitionByPartition() throws ExecutionException, InterruptedException {
        final int partitionCount = 4;
        when(partitionService.getPartitionCount()).thenReturn(partitionCount);
        when(partitionService.getMemberPartitionsMap()).thenReturn(Map.of(member1, List.of(0, 2), member2, List.of(1, 3)));
        when(operationService.invokeOnTargetAsync(any(SearchMemberOperation.class), eq(member1)))
                .thenReturn(CompletableFuture.failedFuture(new MemberLeftException()));
        when(operationService.invokeOnTargetAsync(any(SearchMemberOperation.class), eq(member2)))
                .thenReturn(CompletableFuture.completedFuture(new SearchMemberResult(srs(srData("bad result", 0.1f)),
                        new PartitionIdSet(partitionCount, List.of(1, 3)))));
        when(operationService.invokeOnPartitionAsync(any(SearchOperation.class), eq(0)))
                .thenReturn(CompletableFuture.completedFuture(srs(srData("result", 0.5f))));
        when(operationService.invokeOnPartitionAsync(any(SearchOperation.class), eq(2)))
                .thenReturn(CompletableFuture.completedFuture(srs(srData("better result", 0.6f))));

        var results = searcher.search(name, vectors, options).get();

        assertThat(results.size()).isEqualTo(1);
        assertThat(toMap(results)).containsOnlyKeys(heapData("better result"));

        verify(operationService, times(2).description("Should retry partition"))
                .invokeOnPartitionAsync(any(), anyInt());
    }

    @Test
    void shouldRetryAllFailedMembersPartitionByPartition() throws ExecutionException, InterruptedException {
        final int partitionCount = 4;
        when(partitionService.getPartitionCount()).thenReturn(partitionCount);
        when(partitionService.getMemberPartitionsMap()).thenReturn(Map.of(member1, List.of(0, 2), member2, List.of(1, 3)));
        // failing invocations on all members are very bizarre, but we can handle it
        when(operationService.invokeOnTargetAsync(any(SearchMemberOperation.class), any()))
                .thenReturn(CompletableFuture.failedFuture(new MemberLeftException()));
        when(operationService.invokeOnPartitionAsync(any(SearchOperation.class), anyInt()))
                .thenAnswer(inv -> CompletableFuture.completedFuture(
                        srs(srData("result" + inv.getArgument(1), 0.5f + inv.getArgument(1, Integer.class)))));

        var results = searcher.search(name, vectors, options).get();

        assertThat(results.size()).isEqualTo(1);
        assertThat(toMap(results)).containsOnlyKeys(heapData("result3"));

        verify(operationService, times(4).description("Should retry partitions"))
                .invokeOnPartitionAsync(any(), anyInt());
    }

    @Test
    void shouldRetryFailedMemberPartitionByPartitionAndFailOnRetryFailure() {
        final int partitionCount = 4;
        when(partitionService.getPartitionCount()).thenReturn(partitionCount);
        when(partitionService.getMemberPartitionsMap()).thenReturn(Map.of(member1, List.of(0, 2), member2, List.of(1, 3)));
        when(operationService.invokeOnTargetAsync(any(SearchMemberOperation.class), eq(member1)))
                .thenReturn(CompletableFuture.failedFuture(new MemberLeftException()));
        when(operationService.invokeOnTargetAsync(any(SearchMemberOperation.class), eq(member2)))
                .thenReturn(CompletableFuture.completedFuture(new SearchMemberResult(srs(srData("bad result", 0.1f)),
                        new PartitionIdSet(partitionCount, List.of(1, 3)))));
        when(operationService.invokeOnPartitionAsync(any(SearchOperation.class), eq(0)))
                .thenReturn(CompletableFuture.completedFuture(srs(srData("result", 0.5f))));
        // complete invocation failure should be rare in practice thank to invocation-level retries
        when(operationService.invokeOnPartitionAsync(any(SearchOperation.class), eq(2)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Fatal retry failure")));

        assertThat(searcher.search(name, vectors, options))
                .failsWithin(Duration.ofSeconds(10))
                .withThrowableThat().withMessageContaining("Fatal retry failure");

        verify(operationService, times(2).description("Should retry each partition once"))
                .invokeOnPartitionAsync(any(), anyInt());
    }
}
