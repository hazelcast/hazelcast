/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.mapreduce.impl.task;

import com.hazelcast.mapreduce.impl.MapReduceService;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

public class ReducerTask<Key, Chunk> {

    private final JobSupervisor supervisor;
    private final Queue reducerQueue;
    private final String name;
    private final String jobId;

    public ReducerTask(String name, String jobId, JobSupervisor supervisor, Queue reducerQueue) {
        this.name = name;
        this.jobId = jobId;
        this.supervisor = supervisor;
        this.reducerQueue = reducerQueue;
    }

    public String getName() {
        return name;
    }

    public String getJobId() {
        return jobId;
    }

    public void processChunk(Map<Key, Chunk> chunk) {

    }

}
