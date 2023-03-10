/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.instance.impl.executejar;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class ExecuteJobParameters {

    @Nonnull
    private final String jarPath;
    @Nullable
    private final String snapshotName;
    @Nullable
    private final String jobName;


    public ExecuteJobParameters(@Nonnull String jarPath, @Nullable String snapshotName, @Nullable String jobName) {
        this.jarPath = jarPath;
        this.snapshotName = snapshotName;
        this.jobName = jobName;
    }

    @Nonnull
    public String getJarPath() {
        return jarPath;
    }

    public boolean hasSnapshotName() {
        return snapshotName != null;
    }

    @Nullable
    public String getSnapshotName() {
        return snapshotName;
    }

    public boolean hasJobName() {
        return jobName != null;
    }

    @Nullable
    public String getJobName() {
        return jobName;
    }
}
