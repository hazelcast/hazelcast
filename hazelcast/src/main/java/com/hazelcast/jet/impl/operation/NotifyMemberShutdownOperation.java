/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;

import java.util.concurrent.CompletableFuture;

/**
 * Operation sent from a non-master member to master to notify it that the
 * caller is about to shut down. The master should request termination of all
 * jobs running on caller and then the caller will actually shut down.
 */
public class NotifyMemberShutdownOperation extends AsyncOperation implements AllowedDuringPassiveState {

    public NotifyMemberShutdownOperation() {
    }

    @Override
    protected CompletableFuture<Void> doRun() {
        return getJobCoordinationService().addShuttingDownMember(getCallerUuid());
    }

    @Override
    public int getClassId() {
        return JetInitDataSerializerHook.NOTIFY_MEMBER_SHUTDOWN_OP;
    }
}
