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

import com.hazelcast.jet.impl.JobSummary;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class GetJobSummaryListOperation extends AsyncOperation implements ReadonlyOperation {

    public GetJobSummaryListOperation() {
    }

    @Override
    public CompletableFuture<List<JobSummary>> doRun() {
        return getJobCoordinationService().getJobSummaryList();
    }

    @Override
    public int getClassId() {
        return JetInitDataSerializerHook.GET_JOB_SUMMARY_LIST_OP;
    }
}
