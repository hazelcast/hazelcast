/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cardinality;

import com.hazelcast.cardinality.impl.CardinalityEstimatorContainer;
import com.hazelcast.cardinality.impl.CardinalityEstimatorService;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.Accessors;

import static com.hazelcast.test.Accessors.getFirstBackupInstance;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.Accessors.getPartitionIdViaReflection;

final class CardinalityEstimatorTestUtil {

    private CardinalityEstimatorTestUtil() {
    }

    /**
     * Returns the backup instance of an {@link CardinalityEstimator} by a given cardinality estimator instance.
     * <p>
     * Note: Returns the backups from the first replica index.
     *
     * @param instances            the {@link HazelcastInstance} array to gather the data from
     * @param cardinalityEstimator the {@link CardinalityEstimator} to retrieve the backup from
     * @return the backup estimation
     */
    static long getBackupEstimate(HazelcastInstance[] instances, CardinalityEstimator cardinalityEstimator) {
        int partitionId = getPartitionIdViaReflection(cardinalityEstimator);
        HazelcastInstance backupInstance = getFirstBackupInstance(instances, partitionId);
        return getBackupEstimate(backupInstance, cardinalityEstimator.getName());
    }

    /**
     * Returns the backup estimation of an {@link CardinalityEstimator} by a given cardinality estimator name.
     * <p>
     * Note: You have to provide the {@link HazelcastInstance} you want to retrieve the backups from.
     * Use {@link Accessors#getBackupInstance(HazelcastInstance[], int, int)} to retrieve the backup instance for a given replica index.
     *
     * @param backupInstance the {@link HazelcastInstance} to retrieve the backup estimation from
     * @param estimatorName  the cardinality estimator name
     * @return the backup estimation
     */
    static long getBackupEstimate(HazelcastInstance backupInstance, String estimatorName) {
        NodeEngineImpl nodeEngine = getNodeEngineImpl(backupInstance);
        CardinalityEstimatorService service = nodeEngine.getService(CardinalityEstimatorService.SERVICE_NAME);
        CardinalityEstimatorContainer container = service.getCardinalityEstimatorContainer(estimatorName);
        return container.estimate();
    }
}
