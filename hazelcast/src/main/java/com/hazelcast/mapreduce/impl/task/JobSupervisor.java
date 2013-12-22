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

import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.Reducer;

public class JobSupervisor {

    private final String name;
    private final String jobId;
    private final JobTracker jobTracker;

    public JobSupervisor(String name, String jobId, JobTracker jobTracker) {
        this.name = name;
        this.jobId = jobId;
        this.jobTracker = jobTracker;
    }

    public Reducer getReducerByKey(Object key) {
        return null;
    }

    public void executeMapCombineTask(MapCombineTask mapCombineTask) {

    }

}
