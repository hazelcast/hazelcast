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

package com.hazelcast.client.bluegreen;

import com.hazelcast.client.impl.clientside.CandidateClusterContext;
import com.hazelcast.client.impl.clientside.ClusterDiscoveryService;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.internal.util.MutableInteger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientClusterDiscoveryServiceTest {

    private CandidateClusterContext createContext(int clusterIndex) {
        return new CandidateClusterContext(String.valueOf(clusterIndex), null, null,
                null, null, null);
    }

    private LifecycleService lifecycleService = new AlwaysUpLifecycleService();

    @Test
    public void test_oneIteration() {
        ArrayList<CandidateClusterContext> arrayList = new ArrayList<>();

        int numberOfCandidates = 10;
        for (int i = 0; i < numberOfCandidates; i++) {
            arrayList.add(createContext(i));
        }
        ClusterDiscoveryService discoveryService = new ClusterDiscoveryService(arrayList, 1, lifecycleService);

        MutableInteger count = new MutableInteger();
        discoveryService.tryNextCluster((o, o2) -> {
            count.value++;
            return false;
        });

        assertEquals(numberOfCandidates, count.value);
    }

    @Test
    public void test_no_iteration_when_try_count_is_zero() {
        ArrayList<CandidateClusterContext> arrayList = new ArrayList<>();

        int numberOfCandidates = 10;
        for (int i = 0; i < numberOfCandidates; i++) {
            arrayList.add(createContext(i));
        }
        ClusterDiscoveryService discoveryService = new ClusterDiscoveryService(arrayList, 0, lifecycleService);

        MutableInteger count = new MutableInteger();
        for (int i = 0; i < 3; i++) {
            discoveryService.tryNextCluster((o, o2) -> {
                count.value++;
                return false;
            });
        }

        assertEquals(0, count.value);
    }

    @Test
    public void test_current_candidate_does_not_change_when_try_count_is_zero() {
        ArrayList<CandidateClusterContext> arrayList = new ArrayList<>();

        int numberOfCandidates = 10;
        for (int i = 0; i < numberOfCandidates; i++) {
            arrayList.add(createContext(i));
        }
        ClusterDiscoveryService discoveryService = new ClusterDiscoveryService(arrayList, 0, lifecycleService);

        Set<CandidateClusterContext> candidates = new HashSet<>();
        for (int i = 0; i < 3; i++) {
            candidates.add(discoveryService.current());
        }

        assertTrue(candidates.toString(), candidates.contains(arrayList.get(0)));
    }

    @Test
    public void test_continueFromWhereItleftOff() {
        ArrayList<CandidateClusterContext> arrayList = new ArrayList<>();

        int numberOfCandidates = 10;
        for (int i = 0; i < numberOfCandidates; i++) {
            arrayList.add(createContext(i));
        }
        ClusterDiscoveryService discoveryService = new ClusterDiscoveryService(arrayList, 1, lifecycleService);

        getNextCluster(discoveryService);

        assertEquals(arrayList.get(2), getNextCluster(discoveryService));
        assertEquals(arrayList.get(3), getNextCluster(discoveryService));

    }

    private CandidateClusterContext getNextCluster(ClusterDiscoveryService discoveryService) {
        AtomicReference<CandidateClusterContext> currentCandidate = new AtomicReference<>();
        discoveryService.tryNextCluster((current, next) -> {
            currentCandidate.set(next);
            return true;
        });
        return currentCandidate.get();
    }

    @Test
    public void test_n_iterations() {
        ArrayList<CandidateClusterContext> arrayList = new ArrayList<>();
        int n = 15;

        for (int i = 0; i < 10; i++) {
            arrayList.add(createContext(i));
        }
        ClusterDiscoveryService discoveryService = new ClusterDiscoveryService(arrayList, n, lifecycleService);

        MutableInteger count = new MutableInteger();
        discoveryService.tryNextCluster((o, o2) -> {
            count.value++;
            return false;
        });

        assertEquals(10 * n, count.value);

    }

    @Test
    public void testCurrentAndNextReturnsCorrect() {
        ArrayList<CandidateClusterContext> arrayList = new ArrayList<>();

        CandidateClusterContext first = createContext(1);
        arrayList.add(first);
        CandidateClusterContext second = createContext(2);
        arrayList.add(second);

        ClusterDiscoveryService discoveryService = new ClusterDiscoveryService(arrayList, 10, lifecycleService);

        assertEquals(first, discoveryService.current());

        assertEquals(second, getNextCluster(discoveryService));
        assertEquals(second, discoveryService.current());

        assertEquals(first, getNextCluster(discoveryService));
        assertEquals(first, discoveryService.current());
    }

    @Test
    public void testSingleCandidateBehavior() {
        CandidateClusterContext context = createContext(1);

        ArrayList<CandidateClusterContext> arrayList = new ArrayList<>();
        arrayList.add(context);
        ClusterDiscoveryService discoveryService = new ClusterDiscoveryService(arrayList, 1, lifecycleService);

        assertNotNull(discoveryService.current());

        for (int i = 1; i < 3; i++) {
            assertNotNull(discoveryService.current());
            assertEquals(context, getNextCluster(discoveryService));
        }
    }

    @Test
    public void test_current_and_next_clusters_are_same_when_single_candidate() {
        CandidateClusterContext context = createContext(1);

        ArrayList<CandidateClusterContext> arrayList = new ArrayList<>();
        arrayList.add(context);
        ClusterDiscoveryService discoveryService = new ClusterDiscoveryService(arrayList, 1, lifecycleService);

        Set<CandidateClusterContext> scannedClusters = new HashSet<>();
        discoveryService.tryNextCluster((current, next) -> {
            scannedClusters.add(current);
            scannedClusters.add(next);
            return false;
        });

        assertEquals(1, scannedClusters.size());
        assertTrue(scannedClusters.contains(context));
    }

    @Test
    public void test_current_and_next_clusters_are_same_when_single_candidate_with_multiple_tries() {
        CandidateClusterContext context = createContext(1);

        ArrayList<CandidateClusterContext> arrayList = new ArrayList<>();
        arrayList.add(context);
        ClusterDiscoveryService discoveryService = new ClusterDiscoveryService(arrayList, 7, lifecycleService);

        Set<CandidateClusterContext> scannedClusters = new HashSet<>();
        discoveryService.tryNextCluster((current, next) -> {
            scannedClusters.add(current);
            scannedClusters.add(next);
            return false;
        });

        assertEquals(1, scannedClusters.size());
        assertTrue(scannedClusters.contains(context));
    }

    private class AlwaysUpLifecycleService implements LifecycleService {

        @Override
        public boolean isRunning() {
            return true;
        }

        @Override
        public void shutdown() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void terminate() {
            throw new UnsupportedOperationException();
        }

        @Nonnull
        @Override
        public UUID addLifecycleListener(@Nonnull LifecycleListener lifecycleListener) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean removeLifecycleListener(@Nonnull UUID registrationId) {
            throw new UnsupportedOperationException();
        }
    }
}
