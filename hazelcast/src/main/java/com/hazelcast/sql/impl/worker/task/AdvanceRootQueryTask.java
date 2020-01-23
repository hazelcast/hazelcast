/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.worker.task;

import com.hazelcast.sql.impl.exec.RootExec;

/**
 * The task to get more results from the root executor. Submitted from user thread.
 */
public class AdvanceRootQueryTask implements QueryTask {
    /** Root executor. */
    private final RootExec rootExec;

    public AdvanceRootQueryTask(RootExec rootExec) {
        this.rootExec = rootExec;
    }

    public RootExec getRootExec() {
        return rootExec;
    }
}
