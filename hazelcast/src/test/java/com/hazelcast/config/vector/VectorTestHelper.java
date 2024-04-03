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
        Config config = new Config();
        VectorCollectionConfig vectorCollectionConfig = new VectorCollectionConfig(collectionName);
        VectorIndexConfig indexConfig = new VectorIndexConfig().setName(indexName).setDimension(indexDim).setMetric(indexMetric);
        vectorCollectionConfig.addVectorIndexConfig(indexConfig);
        config.addVectorCollectionConfig(vectorCollectionConfig);
        return vectorCollectionConfig;
    }

    public static VectorIndexConfig buildVectorIndex(String name, int dim, Metric metric) {
        return new VectorIndexConfig().setName(name).setDimension(dim).setMetric(metric);
    }
}
