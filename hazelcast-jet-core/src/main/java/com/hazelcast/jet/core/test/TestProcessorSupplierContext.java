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

package com.hazelcast.jet.core.test;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.core.test.TestSupport.getLogger;

/**
 * {@link ProcessorSupplier.Context} implementation suitable to be used in tests.
 */
public class TestProcessorSupplierContext implements ProcessorSupplier.Context {

    private JetInstance jetInstance;
    private int localParallelism = 1;

    @Override
    public JetInstance jetInstance() {
        return jetInstance;
    }

    /**
     * Set the jet instance.
     */
    public TestProcessorSupplierContext setJetInstance(JetInstance jetInstance) {
        this.jetInstance = jetInstance;
        return this;
    }

    @Override
    public int localParallelism() {
        return localParallelism;
    }

    @Nonnull @Override
    public ILogger logger() {
        return getLogger(getClass());
    }

    /**
     * Set local parallelism.
     */
    public TestProcessorSupplierContext setLocalParallelism(int localParallelism) {
        this.localParallelism = localParallelism;
        return this;
    }
}
