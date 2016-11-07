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

package com.hazelcast.jet.impl.statemachine.jobmanager.events;

import com.hazelcast.jet.impl.executor.Task;
import com.hazelcast.jet.impl.runtime.JobManager;
import com.hazelcast.jet.impl.runtime.VertexRunner;

import java.util.List;
import java.util.function.Consumer;

import static com.hazelcast.jet.impl.util.JetUtil.unchecked;

public class DestroyEventHandler implements Consumer<Void> {
    private final JobManager jobManager;
    private final List<Task> networkTasks;

    public DestroyEventHandler(JobManager jobManager) {
        this.jobManager = jobManager;
        networkTasks = jobManager.getJobContext().getExecutorContext().getNetworkTasks();
    }

    public void accept(Void payload) {
        Throwable error = null;

        try {
            for (VertexRunner runner : jobManager.runners()) {
                try {
                    runner.destroy();
                } catch (Throwable e) {
                    error = e;
                }
            }

            if (error != null) {
                throw unchecked(error);
            }
        } finally {
            networkTasks.forEach(Task::destroy);
        }
    }
}
