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
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.impl.runtime.JobManager;
import com.hazelcast.jet.impl.runtime.jobmanager.JobManagerResponse;
import com.hazelcast.jet.impl.statemachine.jobmanager.requests.InterruptJobRequest;

public class JobInterruptOperation extends AsyncJetOperation {

    @SuppressWarnings("unused")
    public JobInterruptOperation() {
    }

    public JobInterruptOperation(String name) {
        super(name);
    }

    @Override
    public void run() throws Exception {
        JobContext jobContext = getJobContext();

        JobManager jobManager = jobContext.getJobManager();
        ICompletableFuture<JobManagerResponse> interruptFuture = jobManager.handleRequest(
                new InterruptJobRequest()
        );

        interruptFuture.andThen(new JobManagerRequestCallback(this,
                "Unable interrupt job execution", () -> {
            ICompletableFuture<Object> future = jobManager.getInterruptionMailBox();
            if (future == null) {
                sendResponse(null);
                return;
            }
            future.andThen(new ExecutionCallback<Object>() {
                @Override
                public void onResponse(Object response) {
                    sendResponse(response);
                }

                @Override
                public void onFailure(Throwable t) {
                    sendResponse(t);
                }
            });
        }));
    }
}
