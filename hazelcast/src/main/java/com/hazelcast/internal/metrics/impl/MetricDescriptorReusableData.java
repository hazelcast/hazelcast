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

package com.hazelcast.internal.metrics.impl;

import java.util.Arrays;

class MetricDescriptorReusableData {
    private final int allCreatedEndSize;
    private final MetricDescriptorImpl[] pool;
    private final int poolPtr;

    MetricDescriptorReusableData(int allCreatedEndSize, MetricDescriptorImpl[] pool, int poolPtr) {
        this.allCreatedEndSize = allCreatedEndSize;
        this.pool = pool;
        this.poolPtr = poolPtr;
    }

    int getAllCreatedEndSize() {
        return allCreatedEndSize;
    }

    MetricDescriptorImpl[] getPool() {
        return pool;
    }

    int getPoolPtr() {
        return poolPtr;
    }

    void destroy() {
        Arrays.fill(pool, null);
    }
}
