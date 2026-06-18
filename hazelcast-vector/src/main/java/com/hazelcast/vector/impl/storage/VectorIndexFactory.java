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

package com.hazelcast.vector.impl.storage;

import com.hazelcast.config.vector.VectorIndexConfig;

public class VectorIndexFactory {
    private VectorIndexFactory() {
    }

    public static AbstractVectorIndex create(VectorIndexConfig indexConfig) {
        if (indexConfig.isUseDeduplication()) {
            return new VectorIndexMultipleKeys(
                    indexConfig.getName(),
                    indexConfig.getMetric(),
                    indexConfig.getMaxDegree(),
                    indexConfig.getEfConstruction(),
                    indexConfig.getDimension()
            );
        }
        return new VectorIndexSingleKey(
                indexConfig.getName(),
                indexConfig.getMetric(),
                indexConfig.getMaxDegree(),
                indexConfig.getEfConstruction(),
                indexConfig.getDimension()
        );
    }
}
