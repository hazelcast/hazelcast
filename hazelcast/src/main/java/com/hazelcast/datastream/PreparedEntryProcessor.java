/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.datastream;

import com.hazelcast.datastream.impl.DSService;
import com.hazelcast.datastream.impl.entryprocessor.ExecuteEntryProcessorOperationFactory;
import com.hazelcast.spi.OperationService;

import java.util.List;
import java.util.Map;

public class PreparedEntryProcessor<V> {
    private final OperationService operationService;
    private final String preparedId;
    private final String name;

    public PreparedEntryProcessor(OperationService operationService, String name, String preparedId) {
        this.operationService = operationService;
        this.name = name;
        this.preparedId = preparedId;
    }

    public List<V> execute(Map<String, Object> bindings) {
        try {
            operationService.invokeOnAllPartitions(
                    DSService.SERVICE_NAME, new ExecuteEntryProcessorOperationFactory(name, preparedId, bindings));
            return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
