/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.clientside.ClientDiscoveryService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.NoSuchElementException;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientClusterDiscoveryServiceTest {

    private CandidateClusterContext createContext() {
        return new CandidateClusterContext(null, null, null, null, null, null);
    }

    @Test(expected = NoSuchElementException.class)
    public void test_oneIteration() {
        ArrayList<CandidateClusterContext> arrayList = new ArrayList<CandidateClusterContext>();

        int numberOfCandidates = 10;
        for (int i = 0; i < numberOfCandidates; i++) {
            arrayList.add(createContext());
        }
        ClientDiscoveryService discoveryService = new ClientDiscoveryService(1, arrayList);

        int count = 0;
        while (discoveryService.hasNext()) {
            discoveryService.next();
            count++;
        }

        assertEquals(numberOfCandidates, count);
        discoveryService.next();
    }

    @Test
    public void test_continueFromWhereItleftOff() {
        ArrayList<CandidateClusterContext> arrayList = new ArrayList<CandidateClusterContext>();

        int numberOfCandidates = 10;
        for (int i = 0; i < numberOfCandidates; i++) {
            arrayList.add(createContext());
        }
        ClientDiscoveryService discoveryService = new ClientDiscoveryService(1, arrayList);

        discoveryService.next();
        discoveryService.next();

        discoveryService.resetSearch();

        assertEquals(arrayList.get(3), discoveryService.next());

    }

    @Test(expected = NoSuchElementException.class)
    public void test_n_iterations() {
        ArrayList<CandidateClusterContext> arrayList = new ArrayList<CandidateClusterContext>();
        int n = 15;

        for (int i = 0; i < 10; i++) {
            arrayList.add(createContext());
        }
        ClientDiscoveryService discoveryService = new ClientDiscoveryService(n, arrayList);

        int count = 0;
        while (discoveryService.hasNext()) {
            discoveryService.next();
            count++;
        }

        assertEquals(10 * n, count);
        discoveryService.next();

    }

    @Test(expected = NoSuchElementException.class)
    public void test_behaviourWhenBlueGreenIsOff() {
        ArrayList<CandidateClusterContext> arrayList = new ArrayList<CandidateClusterContext>();
        CandidateClusterContext candidateClusterContext = createContext();
        arrayList.add(candidateClusterContext);

        ClientDiscoveryService discoveryService = new ClientDiscoveryService(1, arrayList);

        assertEquals(candidateClusterContext, discoveryService.next());
        assertFalse(discoveryService.hasNext());
        discoveryService.next();

    }

    @Test
    public void testCurrentAndNextReturnsCorrect() {
        ArrayList<CandidateClusterContext> arrayList = new ArrayList<CandidateClusterContext>();

        CandidateClusterContext first = createContext();
        arrayList.add(first);
        CandidateClusterContext second = createContext();
        arrayList.add(second);
        ClientDiscoveryService discoveryService = new ClientDiscoveryService(10, arrayList);

        assertEquals(first, discoveryService.current());
        discoveryService.resetSearch();
        assertTrue(discoveryService.hasNext());

        assertEquals(second, discoveryService.next());
        assertEquals(second, discoveryService.current());

        assertEquals(first, discoveryService.next());
        assertEquals(first, discoveryService.current());
    }

    @Test
    public void testSingleCandidateBehavior() {
        ArrayList<CandidateClusterContext> arrayList = new ArrayList<CandidateClusterContext>();

        arrayList.add(createContext());
        ClientDiscoveryService discoveryService = new ClientDiscoveryService(1, arrayList);

        assertNotNull(discoveryService.current());
        discoveryService.resetSearch();
        assertTrue(discoveryService.hasNext());

        assertNotNull(discoveryService.next());
        assertNotNull(discoveryService.current());
        assertFalse(discoveryService.hasNext());
    }
}
