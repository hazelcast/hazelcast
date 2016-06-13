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

package com.hazelcast.jet.impl.operation;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.jet.impl.container.applicationmaster.ApplicationMasterResponse;
import com.hazelcast.spi.Operation;

class ContainerRequestCallback implements ExecutionCallback<ApplicationMasterResponse> {
    private final Operation operation;
    private final String failureMsg;
    private final Runnable onSuccess;

    public ContainerRequestCallback(Operation operation, String failureMsg, Runnable onSuccess) {
        this.operation = operation;
        this.failureMsg = failureMsg;
        this.onSuccess = onSuccess;
    }

    @Override
    public void onResponse(ApplicationMasterResponse response) {
        if (!response.isSuccess()) {
            operation.sendResponse(new IllegalStateException(failureMsg));
        } else {
            onSuccess.run();
        }
    }

    @Override
    public void onFailure(Throwable t) {
        operation.sendResponse(t);
    }
}
