/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet2;

import com.hazelcast.core.HazelcastInstance;

public interface ProcessorMetaSupplierContext {

    HazelcastInstance getHazelcastInstance();

    int totalParallelism();

    int perNodeParallelism();

    static ProcessorMetaSupplierContext of(HazelcastInstance instance, int totalParallelism, int perNodeParallelism) {
        return new ProcessorMetaSupplierContext() {
            @Override
            public HazelcastInstance getHazelcastInstance() {
                return instance;
            }
            @Override
            public int totalParallelism() {
                return totalParallelism;
            }
            @Override
            public int perNodeParallelism() {
                return perNodeParallelism;
            }
        };
    }
}
