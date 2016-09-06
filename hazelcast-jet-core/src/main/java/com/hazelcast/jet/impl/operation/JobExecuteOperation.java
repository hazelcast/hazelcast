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
import com.hazelcast.jet.impl.statemachine.jobmanager.requests.ExecuteJobRequest;


public class JobExecuteOperation extends AsyncJetOperation {

    @SuppressWarnings("unused")
    public JobExecuteOperation() {
    }

    public JobExecuteOperation(String name) {
        super(name);
    }

    @Override
    public void run() throws Exception {
        JobContext jobContext = getJobContext();
        JobManager jobManager = jobContext.getJobManager();

        getLogger().fine("ExecuteJobRequestOperation.run " + jobContext.getName());

        ICompletableFuture<JobManagerResponse> future = jobManager.handleRequest(new ExecuteJobRequest());

        //Waiting for until all runners started
        future.andThen(new JobManagerRequestCallback(this, "Unable to start runners", () -> {

            //Waiting for execution completion
            final ICompletableFuture<Object> mailboxFuture = jobManager.getExecutionMailBox();
            mailboxFuture.andThen(new ExecutionCallback<Object>() {
                @Override
                public void onResponse(Object o) {
                    sendResponse(o);
                }

                @Override
                public void onFailure(Throwable throwable) {
                    getLogger().info("Operation failed");
                    sendResponse(throwable);
                }
            });
        }));
    }
}
