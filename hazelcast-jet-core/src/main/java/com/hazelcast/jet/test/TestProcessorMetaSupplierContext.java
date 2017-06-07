/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.test;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.ProcessorMetaSupplier;

/**
 * Simple implementation of {@link ProcessorMetaSupplier.Context}.
 */
public class TestProcessorMetaSupplierContext implements ProcessorMetaSupplier.Context {
    private JetInstance jetInstance;
    private int totalParallelism = 1;
    private int localParallelism = 1;

    @Override
    public JetInstance jetInstance() {
        return jetInstance;
    }

    /**
     * Set the jet instance.
     */
    public void setJetInstance(JetInstance jetInstance) {
        this.jetInstance = jetInstance;
    }

    @Override
    public int totalParallelism() {
        return totalParallelism;
    }

    /**
     * Set total parallelism.
     */
    public void setTotalParallelism(int totalParallelism) {
        this.totalParallelism = totalParallelism;
    }

    @Override
    public int localParallelism() {
        return localParallelism;
    }

    /**
     * Set local parallelism.
     */
    public void setLocalParallelism(int localParallelism) {
        this.localParallelism = localParallelism;
    }
}
