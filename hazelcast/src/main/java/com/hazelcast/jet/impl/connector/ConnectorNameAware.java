/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import javax.annotation.Nullable;

/**
 * Implemented by {@link com.hazelcast.jet.core.ProcessorMetaSupplier}s to indicate that they are related to a Source/Sink.
 * That is, they are a connector. {@link com.hazelcast.jet.impl.execution.init.ExecutionPlanBuilder} will handle these PMS
 * and will collect phone home data for connectors.
 */
public interface ConnectorNameAware {
    /**
     * The connector name in the phone home data. that'll be used in the phone homes.
     * See {@link com.hazelcast.jet.pipeline.ConnectorNames}
     */
    @Nullable String getConnectorName();
}
