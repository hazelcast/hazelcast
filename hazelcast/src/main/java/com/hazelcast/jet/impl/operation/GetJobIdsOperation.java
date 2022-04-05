/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
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
public class GetJobIdsOperation extends AsyncOperation implements AllowedDuringPassiveState {

    public static final long ALL_JOBS = Long.MIN_VALUE;

    private String onlyName;
    private long onlyJobId = ALL_JOBS;

    public GetJobIdsOperation() {
    }

    public GetJobIdsOperation(String onlyName, Long onlyJobId) {
        this.onlyName = onlyName;
        this.onlyJobId = onlyJobId == null ? ALL_JOBS : onlyJobId;
    }

    @Override
    public CompletableFuture<GetJobIdsResult> doRun() {
        return getJobCoordinationService().getJobIds(onlyName, onlyJobId);
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
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        onlyName = in.readString();
        onlyJobId = in.readLong();
    }

    public static final class GetJobIdsResult implements IdentifiedDataSerializable {
        public static final GetJobIdsResult EMPTY = new GetJobIdsResult(Collections.emptyList());

        private long[] jobIds;

        /**
         * The coordinator for each job. If null, it's a normal job - the current
         * master is the coordinator.
         * <p>
         * The indexes match those of {@link #jobIds}.
         */
        private boolean[] isLightJobs;

        // for deserialization
        public GetJobIdsResult() { }

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
