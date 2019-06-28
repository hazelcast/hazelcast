/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.worker.data;

import com.hazelcast.sql.impl.exec.RootExec;
import com.hazelcast.sql.impl.worker.data.DataTask;

/**
 * The task to get more results from the root executor. Submitted from user thread.
 */
public class RootDataTask implements DataTask {
    /** Root executor. */
    private final RootExec rootExec;

    public RootDataTask(RootExec rootExec) {
        this.rootExec = rootExec;
    }

    RootExec getRootExec() {
        return rootExec;
    }

    @Override
    public int getThread() {
        return rootExec.getThread();
    }
}
