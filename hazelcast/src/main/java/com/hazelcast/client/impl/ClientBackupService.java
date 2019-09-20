/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl;

import com.hazelcast.client.impl.protocol.task.AddBackupListenerMessageTask;
import com.hazelcast.spi.impl.eventservice.EventPublishingService;

public class ClientBackupService implements EventPublishingService<Long, AddBackupListenerMessageTask.BackupListener> {

    public static final String SERVICE_NAME = "hz:impl:clientBackupService";

    @Override
    public void dispatchEvent(Long event, AddBackupListenerMessageTask.BackupListener listener) {
        listener.onEvent(event);
    }
}
