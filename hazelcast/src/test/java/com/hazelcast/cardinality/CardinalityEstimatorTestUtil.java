/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import static com.hazelcast.test.HazelcastTestSupport.getNodeEngineImpl;

final class CardinalityEstimatorTestUtil {

    private CardinalityEstimatorTestUtil() {
    }

    static long getBackupEstimate(HazelcastInstance backupInstance, String estimatorName) {
        NodeEngineImpl nodeEngine = getNodeEngineImpl(backupInstance);
        CardinalityEstimatorService service = nodeEngine.getService(CardinalityEstimatorService.SERVICE_NAME);
        CardinalityEstimatorContainer container = service.getCardinalityEstimatorContainer(estimatorName);
        return container.estimate();
    }
}
