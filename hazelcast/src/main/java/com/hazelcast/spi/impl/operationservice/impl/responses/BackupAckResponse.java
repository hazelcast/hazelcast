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

package com.hazelcast.spi.impl.operationservice.impl.responses;

import com.hazelcast.spi.impl.operationservice.BackupOperation;

import static com.hazelcast.spi.impl.SpiDataSerializerHook.BACKUP_ACK_RESPONSE;

/**
 * The {Response} for a {@link BackupOperation}. So when a operation like
 * Map.put is done, backup operations are send to the backup partitions. For the initial
 * Map.put to complete, the {@link com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse} needs to return,
 * but also the {@link BackupAckResponse} to make sure that the change
 * is written to the expected number of backups.
 */
public final class BackupAckResponse extends Response {

    // the length of a backup-ack response in bytes.
    public static final int BACKUP_RESPONSE_SIZE_IN_BYTES = RESPONSE_SIZE_IN_BYTES;

    public BackupAckResponse() {
    }

    public BackupAckResponse(long callId, boolean urgent) {
        super(callId, urgent);
    }

    @Override
    public int getClassId() {
        return BACKUP_ACK_RESPONSE;
    }

    @Override
    public String toString() {
        return "BackupAckResponse{callId=" + callId + ", urgent=" + urgent + '}';
    }
}
