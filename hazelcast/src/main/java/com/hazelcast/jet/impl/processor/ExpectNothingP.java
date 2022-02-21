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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;

import javax.annotation.Nonnull;

/**
 * A processor that fails if it receives any input. The error message
 * suggests a different edge configuration should have been used so that no
 * data is received.
 * <p>
 * It ignores any restored state.
 * <p>
 * The processor is designed to be used as a dummy instance where only some
 * members have a real instances of another type.
 *
 * @see ProcessorMetaSupplier#forceTotalParallelismOne(ProcessorSupplier)
 */
public class ExpectNothingP extends AbstractProcessor {

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        throw new IllegalStateException(
                "This vertex has a total parallelism of one and as such only"
                        + " expects input on a specific node. Edge configuration must be adjusted"
                        + " to make sure that only the expected node receives any input."
                        + " Unexpected input received from ordinal " + ordinal + ": " + item
        );
    }

    @Override
    protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        // State might be broadcast to all instances including colleagues of
        // another type - ignore it in the expect-nothing instances
    }
}
