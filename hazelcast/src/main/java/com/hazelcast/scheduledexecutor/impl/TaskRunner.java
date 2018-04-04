/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.scheduledexecutor.impl;

import com.hazelcast.scheduledexecutor.StatefulTask;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import static com.hazelcast.scheduledexecutor.impl.TaskDefinition.Type.SINGLE_RUN;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static java.util.logging.Level.FINEST;
import static java.util.logging.Level.WARNING;

class TaskRunner<V>
        implements Callable<V>, Runnable {

    private final ScheduledExecutorContainer container;

    private final String taskName;

    private final Callable<V> original;

    private final ScheduledTaskDescriptor descriptor;

    private final ScheduledTaskStatisticsImpl statistics;

    private final TaskDefinition.Type type;

    private boolean initted;

    private ScheduledTaskResult resolution;

    TaskRunner(ScheduledExecutorContainer container, ScheduledTaskDescriptor descriptor) {
        this.container = container;
        this.descriptor = descriptor;
        this.original = descriptor.getDefinition().getCommand();
        this.taskName = descriptor.getDefinition().getName();
        this.statistics = descriptor.getStatsSnapshot();
        this.type = descriptor.getDefinition().getType();
        statistics.onInit();
    }

    @Override
    public V call()
            throws Exception {
        beforeRun();
        try {
            V result = original.call();
            if (SINGLE_RUN.equals(type)) {
                resolution = new ScheduledTaskResult(result);
            }
            return result;
        } catch (Throwable t) {
            container.log(WARNING, taskName, "Exception occurred during run", t);
            resolution = new ScheduledTaskResult(t);
            throw rethrow(t);
        } finally {
            afterRun();
        }
    }

    @Override
    public void run() {
        try {
            call();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private void initOnce() {
        if (initted) {
            return;
        }

        Map snapshot = descriptor.getState();
        if (original instanceof StatefulTask && !snapshot.isEmpty()) {
            ((StatefulTask) original).load(snapshot);
        }

        initted = true;
    }

    private void beforeRun() {
        container.log(FINEST, taskName, "Entering running mode");

        try {
            initOnce();
            statistics.onBeforeRun();
        } catch (Exception ex) {
            container.log(WARNING, taskName, "Unexpected exception during beforeRun occurred", ex);
        }
    }

    private void afterRun() {
        try {
            statistics.onAfterRun();

            Map state = new HashMap();
            if (original instanceof StatefulTask) {
                ((StatefulTask) original).save(state);
            }

            container.publishTaskState(taskName, state, statistics.snapshot(), resolution);
        } catch (Exception ex) {
            container.log(WARNING, taskName, "Unexpected exception during afterRun occurred", ex);
        } finally {
            container.notifyResultReady(taskName);
        }

        container.log(FINEST, taskName, "Exiting running mode");
    }

}
