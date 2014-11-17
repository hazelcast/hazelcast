/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.executor.impl.operations;

import com.hazelcast.executor.impl.DistributedExecutorService;
import com.hazelcast.spi.impl.AbstractNamedOperation;

public final class ShutdownOperation extends AbstractNamedOperation {

    public ShutdownOperation() {
    }

    public ShutdownOperation(String name) {
        super(name);
    }

    @Override
    public void run() throws Exception {
        DistributedExecutorService service = getService();
        service.shutdownExecutor(getName());
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return Boolean.TRUE;
    }
}
