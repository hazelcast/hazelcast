/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.client.proxy.listener;

import com.hazelcast.client.connection.Connection;
import com.hazelcast.client.proxy.PartitionClientProxy;
import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.deprecated.nio.Protocol;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.partition.MigrationEvent;
import com.hazelcast.partition.MigrationListener;
import com.hazelcast.partition.MigrationStatus;

public class MigrationEventLRH implements ListenerResponseHandler {
    final MigrationListener listener;
    final PartitionClientProxy partitionClientProxy;

    public MigrationEventLRH(MigrationListener lister, PartitionClientProxy partitionClientProxy) {
        this.listener = lister;
        this.partitionClientProxy = partitionClientProxy;
    }

    public void handleResponse(Protocol response, SerializationService ss) throws Exception {
        int partitionId = Integer.valueOf(response.args[0]);
        Member oldOwner = new MemberImpl(new Address(response.args[1], Integer.valueOf(response.args[2])), false);
        Member newOwner = new MemberImpl(new Address(response.args[3], Integer.valueOf(response.args[4])), false);
        MigrationStatus status = MigrationStatus.valueOf(response.args[5]);
        MigrationEvent event = new MigrationEvent(partitionId, oldOwner, newOwner, status);
        switch (status) {
            case STARTED:
                listener.migrationStarted(event);
                break;
            case COMPLETED:
                listener.migrationCompleted(event);
                break;
            case FAILED:
                listener.migrationFailed(event);
                break;
        }
    }

    public void onError(Connection connection, Exception e) {
        partitionClientProxy.addMigrationListener(listener);
    }
}
