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

package com.hazelcast.internal.networking.nio;

import static java.lang.Thread.currentThread;

/**
 * A {@link Runnable} that gets executed on the {@link NioThread} owning the pipeline.
 *
 * Normally this is a pretty simple task, just schedule the runnable on the owner
 * using {@link NioThread#addTaskAndWakeup(Runnable)}.
 *
 * The problem however is that pipeline migration can cause a task to end up at a
 * NioThread that doesn't own the pipeline any longer. Therefore, this task does a
 * check when it is executed if the owner of the pipeline is the same as the
 * current thread. If it is, then the {@link #run0()} is called. If it isn't, the
 * task is sent to the {@link NioPipeline#ownerAddTaskAndWakeup(Runnable)} which will
 * make sure the task is sent to the right NioThread.
 */
abstract class NioPipelineTask implements Runnable {

    private final NioPipeline pipeline;

    NioPipelineTask(NioPipeline pipeline) {
        this.pipeline = pipeline;
    }

    @Override
    public final void run() {
        if (pipeline.owner() == currentThread()) {
            // the task is executed by the proper thread
            try {
                run0();
            } catch (Exception e) {
                pipeline.onError(e);
            }
        } else {
            // the pipeline is migrating or already has migrated.
            // let's reschedule this task on the pipeline, so it will
            // be picked up by the new owner.
            pipeline.ownerAddTaskAndWakeup(this);
        }
    }

    protected abstract void run0() throws Exception;
}
