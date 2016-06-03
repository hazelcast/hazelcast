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

package com.hazelcast.jet.impl.executor;

import java.util.List;

public class DefaultApplicationTaskContext implements ApplicationTaskContext {
    private final List<Task> tasks;

    public DefaultApplicationTaskContext(List<Task> tasks) {
        this.tasks = tasks;
    }

    @Override
    public void addTask(Task task) {
        this.tasks.add(task);
    }

    @Override
    public Task[] getTasks() {
        return this.tasks.toArray(new Task[this.tasks.size()]);
    }

    @Override
    public void finalizeTasks() {
        for (Task task : this.tasks) {
            task.finalizeTask();
        }
    }

    @Override
    public void init() {
        for (Task task : this.tasks) {
            task.init();
        }
    }

    @Override
    public void interrupt() {
        for (Task task : this.tasks) {
            task.interrupt(null);
        }
    }

    @Override
    public void destroy() {
        for (Task task : this.tasks) {
            task.destroy();
        }
    }
}
