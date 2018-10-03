/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Operation;

import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.impl.util.ExceptionUtil.withTryCatch;

/**
 * Operation sent from a non-master member to master to notify it that the
 * caller is about to shut down. The master should request termination of all
 * jobs running on caller and then the caller will actually shut down.
 */
public class NotifyMemberShutdownOperation extends Operation implements IdentifiedDataSerializable {

    public NotifyMemberShutdownOperation() {
        setWaitTimeout(0);
    }

    @Override
    public void run() {
        JetService service = getService();
        CompletableFuture<Void> future = service.getJobCoordinationService().addShuttingDownMember(getCallerUuid());
        future.whenComplete(withTryCatch(getLogger(), (r, e) -> sendResponse(null)));
    }

    @Override
    public final int getFactoryId() {
        return JetInitDataSerializerHook.FACTORY_ID;
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public int getId() {
        return JetInitDataSerializerHook.NOTIFY_MEMBER_SHUTDOWN_OP;
    }
}
