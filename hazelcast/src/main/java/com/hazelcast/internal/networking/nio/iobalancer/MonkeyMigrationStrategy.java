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

package com.hazelcast.internal.networking.nio.iobalancer;

import com.hazelcast.internal.networking.nio.MigratablePipeline;

import java.util.Iterator;
import java.util.Random;
import java.util.Set;

/**
 * IOBalancer Migration Strategy intended to be used by stress tests only. It always tries to
 * select a random {@link MigratablePipeline pipeline} to be migrated.
 *
 * It stresses the pipeline migration mechanism increasing a chance to reveal possible race-conditions.
 */
class MonkeyMigrationStrategy implements MigrationStrategy {

    private final Random random = new Random();

    @Override
    public boolean imbalanceDetected(LoadImbalance imbalance) {
        Set<? extends MigratablePipeline> candidates = imbalance.getPipelinesOwnedBy(imbalance.srcOwner);
        //only attempts to migrate if at least 1 pipeline exists
        return candidates.size() > 0;
    }

    @Override
    public MigratablePipeline findPipelineToMigrate(LoadImbalance imbalance) {
        Set<? extends MigratablePipeline> candidates = imbalance.getPipelinesOwnedBy(imbalance.srcOwner);
        int pipelineCount = candidates.size();
        int selected = random.nextInt(pipelineCount);
        Iterator<? extends MigratablePipeline> iterator = candidates.iterator();
        for (int i = 0; i < selected; i++) {
            iterator.next();
        }
        return iterator.next();
    }
}
