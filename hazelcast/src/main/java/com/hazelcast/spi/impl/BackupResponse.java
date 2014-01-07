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

package com.hazelcast.spi.impl;

/**
 * The {Response} for a {@link com.hazelcast.spi.BackupOperation}. So when a operation like
 * Map.put is done, backup operations are send to the backup partitions. For the initial
 * Map.put to complete, the {@link com.hazelcast.spi.impl.NormalResponse} needs to return,
 * but also the {@link com.hazelcast.spi.impl.BackupResponse} to make sure that the change
 * is written to the expected number of backups.
 */
final class BackupResponse extends Response {

    public BackupResponse() {
    }

    public BackupResponse(long callId, boolean urgent) {
        super(callId, urgent);
    }

    @Override
    public int getId() {
        return SpiDataSerializerHook.BACKUP_RESPONSE;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("BackupResponse");
        sb.append("{callId=").append(callId);
        sb.append(", urgent=").append(urgent);
        sb.append('}');
        return sb.toString();
    }
}
