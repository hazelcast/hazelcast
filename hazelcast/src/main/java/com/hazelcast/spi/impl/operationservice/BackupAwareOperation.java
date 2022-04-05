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

package com.hazelcast.spi.impl.operationservice;

/**
 * A BackupAwareOperation is an Operation to indicate then when a change is made, a
 * {@link BackupOperation} is created to replicate the backup.
 *
 * @author mdogan 12/6/12
 */
public interface BackupAwareOperation extends PartitionAwareOperation {

    /**
     * Checks if a backup needs to be made.
     * <p>
     * If a call has not made any change, e.g. an AtomicLong increment with 0, no backup needs to be made.
     *
     * @return true if a backup needs to be made, false otherwise.
     */
    boolean shouldBackup();

    /**
     * The synchronous backup count. If no backups need to be made, 0 is returned.
     *
     * @return the synchronous backup count.
     */
    int getSyncBackupCount();

    /**
     * The asynchronous backup count. If no asynchronous backups need to be made, 0 is returned.
     *
     * @return the asynchronous backup count.
     */
    int getAsyncBackupCount();

    /**
     * Creates the {@link BackupOperation} responsible for making the backup.
     *
     * @return the created {@link BackupOperation} responsible for making the backup.
     */
    Operation getBackupOperation();
}
