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
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.Util.idToString;

/**
 * Get the job IDs. Runs in 3 modes:
 * <ul>
 *     <li>get all job ids (if {@link #onlyName} == null && {@link #onlyJobId}
 *          == MIN_VALUE)
 *     <li>get all job ids with a name (if {@link #onlyName} != null)
 *     <li>get a single job id (if {@link #onlyJobId} != MIN_VALUE)
 * </ul>
 */
public class GetJobIdsOperation extends AsyncMasterAwareOperation implements AllowedDuringPassiveState, Versioned {

    public static final long ALL_JOBS = Long.MIN_VALUE;

    private String onlyName;
    private long onlyJobId;
    private boolean includeOnlyLightJobs;
    private transient boolean isRequiredMasterExecution;

    public GetJobIdsOperation() {
    }

    // Possible combinations of input arguments:
    // - If onlyName != null, then onlyJobId and includeOnlyLightJobs are not applicable.
    // - If onlyJobId != null and onlyJobId != ALL_JOBS, then includeOnlyLightJobs is not applicable.
    public GetJobIdsOperation(String onlyName, long onlyJobId, boolean includeOnlyLightJobs) {
        this.onlyName = onlyName;
        this.onlyJobId = onlyJobId;
        this.includeOnlyLightJobs = includeOnlyLightJobs;
    }

    @Override
    public CompletableFuture<GetJobIdsResult> doRun() {
        if (onlyName != null) {
            return getJobCoordinationService().getNormalJobIdsByName(onlyName);
        }
        if (onlyJobId != ALL_JOBS) {
            return getJobCoordinationService().getJobIdById(onlyJobId);
        }
        if (includeOnlyLightJobs) {
            return getJobCoordinationService().getLightJobIds();
        }
        return getJobCoordinationService().getAllJobsId();
    }

    @Override
    public int getClassId() {
        return JetInitDataSerializerHook.GET_JOB_IDS;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeString(onlyName);
        out.writeLong(onlyJobId);
        // RU_COMPAT_5_5
        if (out.getVersion().isGreaterOrEqual(Versions.V5_6)) {
            out.writeBoolean(includeOnlyLightJobs);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        onlyName = in.readString();
        onlyJobId = in.readLong();
        // RU_COMPAT_5_5
        if (in.getVersion().isGreaterOrEqual(Versions.V5_6)) {
            includeOnlyLightJobs = in.readBoolean();
            // Execution on the master is required only when a named normal job or all jobs are requested
            isRequiredMasterExecution = onlyName != null || !includeOnlyLightJobs;
        } else {
            includeOnlyLightJobs = false;
            isRequiredMasterExecution = onlyName != null;
        }
    }

    @Override
    public boolean isRequireMasterExecution() {
        return isRequiredMasterExecution;
    }

    @Override
    public String getServiceName() {
        return JetServiceBackend.SERVICE_NAME;
    }

    public static final class GetJobIdsResult implements IdentifiedDataSerializable {
        public static final GetJobIdsResult EMPTY = new GetJobIdsResult(Collections.emptyList());

        private long[] jobIds;

        /**
         * Indicates whether each job is a light job.
         * The array index corresponds to the same index in {@link #jobIds}.
         */
        private boolean[] isLightJobs;

        // for deserialization
        public GetJobIdsResult() {
        }

        public GetJobIdsResult(long jobId, boolean isLightJob) {
            jobIds = new long[]{jobId};
            isLightJobs = new boolean[]{isLightJob};
        }

        public GetJobIdsResult(List<Tuple2<Long, Boolean>> result) {
            jobIds = new long[result.size()];
            isLightJobs = new boolean[result.size()];
            for (int i = 0; i < result.size(); i++) {
                Tuple2<Long, Boolean> tuple = result.get(i);
                assert tuple.f0() != null && tuple.f1() != null;
                jobIds[i] = tuple.f0();
                isLightJobs[i] = tuple.f1();
            }
        }

        public long[] getJobIds() {
            return jobIds;
        }

        public boolean[] getIsLightJobs() {
            return isLightJobs;
        }

        @Override
        public int getFactoryId() {
            return JetInitDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return JetInitDataSerializerHook.GET_JOB_IDS_RESULT;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeLongArray(jobIds);
            out.writeBooleanArray(isLightJobs);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            jobIds = in.readLongArray();
            isLightJobs = in.readBooleanArray();
        }

        @Override
        public String toString() {
            return "GetJobIdsResult{" +
                    "jobs=" + formatJobs(false) +
                    ", lightJobs=" + formatJobs(true) + '}';
        }

        private String formatJobs(boolean isLightJob) {
            return IntStream.range(0, jobIds.length)
                    .filter(i -> isLightJobs[i] == isLightJob)
                    .mapToObj(i -> idToString(jobIds[i]))
                    .collect(Collectors.joining(", ", "[", "]"));
        }
    }
}
