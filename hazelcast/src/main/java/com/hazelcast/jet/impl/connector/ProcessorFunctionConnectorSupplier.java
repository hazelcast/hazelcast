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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.security.PermissionsUtil;

import javax.annotation.Nonnull;

/**
 * A ProcessorSupplier that checks permission of processor function
 */
class ProcessorFunctionConnectorSupplier extends AbstractHazelcastConnectorSupplier {

    private static final long serialVersionUID = 1L;

    private final FunctionEx<HazelcastInstance, Processor> processorFunction;

    ProcessorFunctionConnectorSupplier(
            String dataConnectionName,
            String clientXml,
            FunctionEx<HazelcastInstance, Processor> processorFunction
    ) {
        super(dataConnectionName, clientXml);
        this.processorFunction = processorFunction;
    }

    @Override
    public void init(@Nonnull Context context) {
        PermissionsUtil.checkPermission(processorFunction, context);
        super.init(context);
    }

    @Override
    protected Processor createProcessor(HazelcastInstance instance, SerializationService serializationService) {
        return processorFunction.apply(instance);
    }
}
