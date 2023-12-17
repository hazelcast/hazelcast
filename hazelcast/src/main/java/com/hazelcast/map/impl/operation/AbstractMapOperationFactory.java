/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.operation;

import com.hazelcast.internal.namespace.NamespaceUtil;
import com.hazelcast.internal.namespace.impl.NodeEngineThreadLocalContext;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.OperationFactory;

import java.util.concurrent.Callable;

public abstract class AbstractMapOperationFactory implements OperationFactory {

    protected String name;

    protected AbstractMapOperationFactory() {
    }

    protected AbstractMapOperationFactory(String name) {
        this.name = name;
    }

    @Override
    public final int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    public String getName() {
        return name;
    }

    /**
     * Utility method for executing code within the context of the Namespace associated
     * with IMaps - if one does not exist, this code is executed as if it were called
     * directly in place of this method.
     */
    protected <T> T callWithNamespaceAwareness(Callable<T> callable) {
        NodeEngine engine = NodeEngineThreadLocalContext.getNodeEngineThreadLocalContext();
        String namespace = MapService.lookupNamespace(engine, name);
        return NamespaceUtil.callWithNamespace(engine, namespace, callable);
    }
}
