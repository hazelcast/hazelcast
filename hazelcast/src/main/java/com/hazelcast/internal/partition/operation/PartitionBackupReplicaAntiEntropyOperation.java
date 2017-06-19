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

package com.hazelcast.internal.partition.operation;

import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.NonFragmentedServiceNamespace;
import com.hazelcast.internal.partition.ReplicaErrorLogger;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.PartitionReplicaManager;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.ServiceNamespace;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static com.hazelcast.internal.partition.impl.PartitionDataSerializerHook.PARTITION_BACKUP_REPLICA_ANTI_ENTROPY;

// should not be an urgent operation. required to be in order with backup operations on target node
public final class PartitionBackupReplicaAntiEntropyOperation
        extends AbstractPartitionOperation
        implements PartitionAwareOperation, AllowedDuringPassiveState, Versioned {

    private Map<ServiceNamespace, Long> versions;
    private boolean returnResponse;
    private boolean response = true;

    public PartitionBackupReplicaAntiEntropyOperation() {
    }

    public PartitionBackupReplicaAntiEntropyOperation(Map<ServiceNamespace, Long> versions, boolean returnResponse) {
        this.versions = versions;
        this.returnResponse = returnResponse;
    }

    @Override
    public void run() throws Exception {
        InternalPartitionServiceImpl partitionService = getService();
        int partitionId = getPartitionId();
        int replicaIndex = getReplicaIndex();
        PartitionReplicaManager replicaManager = partitionService.getReplicaManager();

        replicaManager.retainNamespaces(partitionId, versions.keySet());

        Iterator<Map.Entry<ServiceNamespace, Long>> iter = versions.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<ServiceNamespace, Long> entry = iter.next();
            ServiceNamespace ns = entry.getKey();
            long primaryVersion = entry.getValue();

            long[] currentVersions = replicaManager.getPartitionReplicaVersions(partitionId, ns);
            long currentVersion = currentVersions[replicaIndex - 1];

            if (replicaManager.isPartitionReplicaVersionDirty(partitionId, ns) || currentVersion != primaryVersion) {
                logBackupVersionMismatch(ns, currentVersion, primaryVersion);
                continue;
            }
            iter.remove();
        }

        if (!versions.isEmpty()) {
            replicaManager.triggerPartitionReplicaSync(partitionId, versions.keySet(), replicaIndex);
            response = false;
        }
    }

    private void logBackupVersionMismatch(ServiceNamespace ns, long currentVersion, long primaryVersion) {
        ILogger logger = getLogger();
        if (logger.isFinestEnabled()) {
            logger.finest("partitionId=" + getPartitionId() + ", replicaIndex=" + getReplicaIndex()
                    + ", ns=" + ns + " version is not matching to version of the owner or replica is marked as dirty! "
                    + " Expected-version=" + primaryVersion + ", Current-version=" + currentVersion);
        }
    }

    @Override
    public boolean returnsResponse() {
        return returnResponse;
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public boolean validatesTarget() {
        return false;
    }

    @Override
    public String getServiceName() {
        return InternalPartitionService.SERVICE_NAME;
    }

    @Override
    public void logError(Throwable e) {
        ReplicaErrorLogger.log(e, getLogger());
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        if (out.getVersion().isGreaterOrEqual(Versions.V3_9)) {
            out.writeInt(versions.size());
            for (Map.Entry<ServiceNamespace, Long> entry : versions.entrySet()) {
                out.writeObject(entry.getKey());
                out.writeLong(entry.getValue());
            }
        } else {
            assert versions.size() == 1 : "Only single namespace is allowed before V3.9: " + versions.keySet();
            out.writeLong(versions.get(NonFragmentedServiceNamespace.INSTANCE));
        }
        out.writeBoolean(returnResponse);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        if (in.getVersion().isGreaterOrEqual(Versions.V3_9)) {
            int len = in.readInt();
            versions = new HashMap<ServiceNamespace, Long>(len);
            for (int i = 0; i < len; i++) {
                ServiceNamespace ns = in.readObject();
                long v = in.readLong();
                versions.put(ns, v);
            }
        } else {
            versions = new HashMap<ServiceNamespace, Long>(1);
            long v = in.readLong();
            versions.put(NonFragmentedServiceNamespace.INSTANCE, v);
        }
        returnResponse = in.readBoolean();
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", versions=").append(versions);
    }

    @Override
    public int getId() {
        return PARTITION_BACKUP_REPLICA_ANTI_ENTROPY;
    }
}
