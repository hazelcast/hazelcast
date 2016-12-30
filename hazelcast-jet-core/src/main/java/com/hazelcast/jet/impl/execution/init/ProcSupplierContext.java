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

package com.hazelcast.jet.impl.execution.init;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.ProcessorSupplier.Context;

/**
 * Simple implementation of ProcessorSupplier's initialization context object.
 */
class ProcSupplierContext implements Context {
    private final JetInstance instance;
    private final int perNodeParallelism;

    ProcSupplierContext(JetInstance instance, int perNodeParallelism) {
        this.instance = instance;
        this.perNodeParallelism = perNodeParallelism;
    }

    @Override
    public JetInstance getJetInstance() {
        return instance;
    }

    @Override
    public int localParallelism() {
        return perNodeParallelism;
    }
}
