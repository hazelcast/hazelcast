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

package com.hazelcast.util.executor;

import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.Executor;

/**
 * @mdogan 4/9/13
 */
public final class ScheduledTaskRunner implements Runnable {

    private final Executor executor;
    private final Runnable runnable;

    public ScheduledTaskRunner(Executor executor, Runnable runnable) {
        this.executor = executor;
        this.runnable = runnable;
    }

    public void run() {
        try {
            executor.execute(runnable);
        } catch (Throwable t) {
            ExceptionUtil.sneakyThrow(t);
        }
    }
}
