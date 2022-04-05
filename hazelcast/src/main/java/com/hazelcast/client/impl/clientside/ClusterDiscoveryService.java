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

package com.hazelcast.client.impl.clientside;

import com.hazelcast.core.LifecycleService;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkTrue;

public class ClusterDiscoveryService {

    private final int maxTryCount;
    private final LifecycleService lifecycleService;
    private final List<CandidateClusterContext> candidateClusters;
    private final AtomicInteger index = new AtomicInteger(0);

    public ClusterDiscoveryService(List<CandidateClusterContext> candidateClusters,
                                   int maxTryCount, LifecycleService lifecycleService) {
        checkNotNull(candidateClusters, "candidateClusters cannot be null");
        checkTrue(!candidateClusters.isEmpty(), "candidateClusters cannot be empty");
        checkTrue(maxTryCount >= 0, "maxTryCount must be >= 0");

        this.candidateClusters = candidateClusters;
        this.maxTryCount = maxTryCount;
        this.lifecycleService = lifecycleService;
    }

    public boolean tryNextCluster(BiFunction<CandidateClusterContext, CandidateClusterContext, Boolean> function) {
        int tryCount = 0;
        while (lifecycleService.isRunning() && tryCount++ < maxTryCount) {
            for (int i = 0; i < candidateClusters.size(); i++) {
                if (function.apply(current(), next())) {
                    return true;
                }
            }
        }
        return false;
    }

    public CandidateClusterContext current() {
        return candidateClusters.get(index.get() % candidateClusters.size());
    }

    private CandidateClusterContext next() {
        return candidateClusters.get(index.incrementAndGet() % candidateClusters.size());
    }

    public void shutdown() {
        for (CandidateClusterContext discoveryService : candidateClusters) {
            discoveryService.getCredentialsFactory().destroy();
        }
    }
}
