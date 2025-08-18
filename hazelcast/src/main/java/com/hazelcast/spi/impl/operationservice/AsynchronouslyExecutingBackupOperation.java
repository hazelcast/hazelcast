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

package com.hazelcast.spi.impl.operationservice;

import com.hazelcast.spi.impl.operationservice.impl.operations.Backup;

import java.util.function.Consumer;

/**
 * SPI for blocking or offloaded backup operations. Such operations should
 * send backup ACK on their own after they have finished execution.
 *
 * @see BlockingOperation
 * @see Offload
 * @since 5.6
 */
public interface AsynchronouslyExecutingBackupOperation {
    /**
     * This method is used to inject {@link Backup#afterRun()} to offloaded operation.
     * <p>
     * Its goal is to call it on completion of offloaded backup operation.
     *
     * @param backupOpAfterRun {@link Backup#afterRun()}
     */
    void setBackupOpAfterRun(Consumer<Operation> backupOpAfterRun);
}
