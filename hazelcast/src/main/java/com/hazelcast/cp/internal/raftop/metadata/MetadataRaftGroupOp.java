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

package com.hazelcast.cp.internal.raftop.metadata;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.MetadataRaftGroupManager;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.RaftService;

import static com.hazelcast.internal.util.Preconditions.checkTrue;

/**
 * Base class for the {@link RaftOp} impls that are committed to the Metadata group.
 * It verifies that the operation is actually committed to the Metadata group.
 */
public abstract class MetadataRaftGroupOp extends RaftOp {

    @Override
    public final Object run(CPGroupId groupId, long commitIndex) throws Exception {
        RaftService service = getService();
        MetadataRaftGroupManager metadataGroupManager = service.getMetadataGroupManager();
        checkTrue(metadataGroupManager.getMetadataGroupId().equals(groupId),
                "Cannot perform CP Subsystem management call on " + groupId);
        return run(metadataGroupManager, commitIndex);
    }

    public abstract Object run(MetadataRaftGroupManager metadataGroupManager, long commitIndex) throws Exception ;

    @Override
    public final String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

}
