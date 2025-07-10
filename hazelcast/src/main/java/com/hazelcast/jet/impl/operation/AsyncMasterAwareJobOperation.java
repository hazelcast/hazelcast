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

package com.hazelcast.jet.impl.operation;

import com.hazelcast.internal.cluster.impl.MasterNodeChangedException;

/**
 * An abstract class for asynchronous job operations that are aware of the master node.
 * It ensures that the operation is executed on the master node if required.
 */
public abstract class AsyncMasterAwareJobOperation extends AsyncJobOperation implements MasterAwareOperation {

    public AsyncMasterAwareJobOperation(long jobId) {
        super(jobId);
    }

    public AsyncMasterAwareJobOperation() {
        super();
    }

    @Override
    public void beforeRun() throws Exception {
        if (isRequireMasterExecution() && !isMaster()) {
            throw new MasterNodeChangedException("This operation can only be executed on the master node.");
        }
        super.beforeRun();
    }

    private boolean isMaster() {
        return getNodeEngine().getClusterService().isMaster();
    }
}
