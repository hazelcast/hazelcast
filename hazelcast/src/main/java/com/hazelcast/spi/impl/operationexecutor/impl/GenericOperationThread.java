/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.operationexecutor.impl;

import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.instance.NodeExtension;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;

/**
 * An {@link OperationThread} for non partition specific operations.
 */
public final class GenericOperationThread extends OperationThread {

    private final OperationRunner operationRunner;

    public GenericOperationThread(String name, int threadId, OperationQueue queue, ILogger logger,
                                  HazelcastThreadGroup threadGroup, NodeExtension nodeExtension,
                                  OperationRunner operationRunner, boolean priority) {
        super(name, threadId, queue, logger, threadGroup, nodeExtension, priority);
        this.operationRunner = operationRunner;
    }

    @Override
    public OperationRunner getOperationRunner(int partitionId) {
        return operationRunner;
    }
}
