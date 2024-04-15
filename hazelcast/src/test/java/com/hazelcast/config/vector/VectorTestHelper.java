/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config.vector;

import com.hazelcast.config.Config;

public final class VectorTestHelper {
    public static VectorCollectionConfig buildVectorCollectionConfig(
            String collectionName,
            String indexName,
            int indexDim,
            Metric indexMetric
    ) {
        return buildVectorCollectionConfig(collectionName, indexName, indexDim, indexMetric, null, null, false);
    }

    public static VectorCollectionConfig buildVectorCollectionConfig(
            String collectionName,
            String indexName,
            int indexDim,
            Metric indexMetric,
            Integer maxDegree,
            Integer efConstruction,
            boolean useDeduplication
    ) {
        Config config = new Config();
        VectorCollectionConfig vectorCollectionConfig = new VectorCollectionConfig(collectionName);
        VectorIndexConfig indexConfig = new VectorIndexConfig()
                .setName(indexName)
                .setDimension(indexDim)
                .setMetric(indexMetric)
                .setUseDeduplication(useDeduplication);
        if (maxDegree != null) {
            indexConfig.setMaxDegree(maxDegree);
        }
        if (efConstruction != null) {
            indexConfig.setEfConstruction(efConstruction);
        }
        vectorCollectionConfig.addVectorIndexConfig(indexConfig);
        config.addVectorCollectionConfig(vectorCollectionConfig);
        return vectorCollectionConfig;
    }
}
