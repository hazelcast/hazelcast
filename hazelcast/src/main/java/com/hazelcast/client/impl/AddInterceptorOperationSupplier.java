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

package com.hazelcast.client.impl;

import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.impl.operation.AddInterceptorOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.util.function.Supplier;

public class AddInterceptorOperationSupplier implements Supplier<Operation> {

    private final String id;
    private final String name;
    private final MapInterceptor mapInterceptor;

    public AddInterceptorOperationSupplier(String id, String name, MapInterceptor mapInterceptor) {
        this.id = id;
        this.name = name;
        this.mapInterceptor = mapInterceptor;
    }

    @Override
    public Operation get() {
        return new AddInterceptorOperation(id, mapInterceptor, name);
    }
}
