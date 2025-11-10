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

package com.hazelcast.jet.impl.operation;

import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.jet.impl.JobAndSqlSummary;
import com.hazelcast.jet.impl.JobAndSqlSummaryIds;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;
import com.hazelcast.version.Version;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.stream.Collectors.toList;

public class GetJobAndSqlSummaryListOperation extends AsyncOperation implements ReadonlyOperation {

    public GetJobAndSqlSummaryListOperation() {
    }

    @Override
    public CompletableFuture<? extends List<? extends JobAndSqlSummary>> doRun() {
        Version currentClusterVersion = getNodeEngine().getClusterService().getClusterVersion();
        CompletableFuture<List<JobAndSqlSummaryIds>> summaries = getJobCoordinationService().getJobAndSqlSummaryList();
        // TODO RU_COMPAT 5.6 is the last version to (inadvertently) use compact serialization for the response. Going
        //  forward we return a class implementing IdentifiedDataSerializable. This switch is kept for backwards compatibility
        //  during RU. Should be cleaned up when RU from version <= 5.6 is no longer supported.
        //  .collect(toList()) used instead of .toList() as the latter returns an immutable list impl which fails to serialize
        //  correctly whereas the former returns ArrayList.
        return currentClusterVersion.isGreaterOrEqual(Versions.V5_7) ? summaries : summaries.thenApply(
                result -> result.stream().map(JobAndSqlSummaryIds::toOldVersion).collect(toList()));
    }

    @Override
    public int getClassId() {
        return JetInitDataSerializerHook.GET_JOB_AND_SQL_SUMMARY_LIST_OP;
    }
}
