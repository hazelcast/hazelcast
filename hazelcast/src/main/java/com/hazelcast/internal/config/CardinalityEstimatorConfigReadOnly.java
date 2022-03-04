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

package com.hazelcast.internal.config;

import com.hazelcast.config.CardinalityEstimatorConfig;
import com.hazelcast.config.MergePolicyConfig;

public class CardinalityEstimatorConfigReadOnly extends CardinalityEstimatorConfig {

    public CardinalityEstimatorConfigReadOnly(CardinalityEstimatorConfig config) {
        super(config);
    }

    @Override
    public CardinalityEstimatorConfig setName(String name) {
        throw new UnsupportedOperationException("This config is read-only cardinality estimator: " + getName());
    }

    @Override
    public CardinalityEstimatorConfig setBackupCount(int backupCount) {
        throw new UnsupportedOperationException("This config is read-only cardinality estimator: " + getName());
    }

    @Override
    public CardinalityEstimatorConfig setAsyncBackupCount(int asyncBackupCount) {
        throw new UnsupportedOperationException("This config is read-only cardinality estimator: " + getName());
    }

    @Override
    public CardinalityEstimatorConfig setSplitBrainProtectionName(String splitBrainProtectionName) {
        throw new UnsupportedOperationException("This config is read-only cardinality estimator: " + getName());
    }

    @Override
    public CardinalityEstimatorConfig setMergePolicyConfig(MergePolicyConfig mergePolicyConfig) {
        throw new UnsupportedOperationException("This config is read-only cardinality estimator: " + getName());
    }
}
