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

package com.hazelcast.spi.impl.merge;

import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.merge.MergingValue;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;

import static org.junit.Assert.assertNotNull;

class TestContainerMerger extends AbstractContainerMerger<Object, Object, MergingValue<Object>> {

    private final TestMergeOperation mergeOperation;

    TestContainerMerger(TestContainerCollector collector, NodeEngine nodeEngine, TestMergeOperation mergeOperation) {
        super(collector, nodeEngine);
        this.mergeOperation = mergeOperation;
    }

    @Override
    protected String getLabel() {
        return null;
    }

    @Override
    protected void runInternal() {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig();
        SplitBrainMergePolicy mergePolicy = getMergePolicy(mergePolicyConfig);
        assertNotNull("Expected to retrieve a merge policy, but was null", mergePolicy);

        if (mergeOperation != null) {
            invoke(MapService.SERVICE_NAME, mergeOperation, 1);
        }
    }
}
