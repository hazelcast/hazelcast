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
import com.hazelcast.datastream.impl.query.ExecuteQueryOperationFactory;
import com.hazelcast.spi.OperationService;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class PreparedQuery<V> {

    private final OperationService operationService;
    private final String preparedId;
    private final String name;

    public PreparedQuery(OperationService operationService, String name, String preparedId) {
        this.operationService = operationService;
        this.name = name;
        this.preparedId = preparedId;
    }

    public List<V> execute(Map<String, Object> bindings) {
        try {
            List<V> result = new LinkedList<>();
            Map<Integer, Object> r = operationService.invokeOnAllPartitions(
                    DSService.SERVICE_NAME, new ExecuteQueryOperationFactory(name, preparedId, bindings));

            for (Object v : r.values()) {
                result.addAll((List) v);
            }
            return result;
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }
}
