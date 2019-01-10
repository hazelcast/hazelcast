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

package com.hazelcast.client.impl.clientside;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class ClientDiscoveryService implements Iterator<CandidateClusterContext> {

    private final ArrayList<CandidateClusterContext> discoveryServices;
    private final long size;
    private long tail;
    private long head;


    public ClientDiscoveryService(ArrayList<CandidateClusterContext> discoveryServices) {
        this.discoveryServices = discoveryServices;
        this.size = discoveryServices.size();
    }

    public void resetSearch() {
        tail = head;
    }

    public boolean hasNext() {
        return head != tail + size;
    }

    public CandidateClusterContext next() {
        if (head == tail + size) {
            throw new NoSuchElementException("Has no alternative cluster");
        }
        CandidateClusterContext candidateClusterContext = discoveryServices.get((int) (head % size));
        head++;
        return candidateClusterContext;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove");
    }
}
