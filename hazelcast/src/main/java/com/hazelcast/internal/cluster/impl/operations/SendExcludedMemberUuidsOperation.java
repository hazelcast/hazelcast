/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.cluster.impl.operations;

import com.hazelcast.hotrestart.InternalHotRestartService;
import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.SetUtil;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;

/**
 * Sends excluded member uuids to a member which is in that set.
 * We need this operation because we don't allow an excluded member to join to the cluster.
 * Therefore, we notify it so that the excluded member can force-start itself.
 */
public class SendExcludedMemberUuidsOperation extends AbstractClusterOperation {


    private Set<String> excludedMemberUuids;

    public SendExcludedMemberUuidsOperation() {
    }

    public SendExcludedMemberUuidsOperation(Set<String> excludedMemberUuids) {
        this.excludedMemberUuids = excludedMemberUuids != null ? excludedMemberUuids : Collections.<String>emptySet();
    }

    @Override
    public void run() {
        final NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        final InternalHotRestartService hotRestartService = nodeEngine.getNode().getNodeExtension()
                                                                      .getInternalHotRestartService();
        hotRestartService.handleExcludedMemberUuids(getCallerAddress(), excludedMemberUuids);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(excludedMemberUuids.size());
        for (String uuid : excludedMemberUuids) {
            out.writeUTF(uuid);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        Set<String> excludedMemberUuids = SetUtil.createHashSet(size);
        for (int i = 0; i < size; i++) {
            excludedMemberUuids.add(in.readUTF());
        }

        this.excludedMemberUuids = unmodifiableSet(excludedMemberUuids);
    }

    @Override
    public int getId() {
        return ClusterDataSerializerHook.SEND_EXCLUDED_MEMBER_UUIDS;
    }

}
