/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.hotrestart;

/**
 * @deprecated since 5.0 because of package renaming
 * Use {@link com.hazelcast.persistence.BackupTaskState} instead.
 */
@Deprecated
public enum BackupTaskState {
    /** No backup task has yet been run */
    NO_TASK,
    /** The backup task has been submitted but not yet started. */
    NOT_STARTED,
    /** The backup task is currently in progress */
    IN_PROGRESS,
    /** The backup task has failed */
    FAILURE,
    /** The backup task completed successfully */
    SUCCESS;

    /**
     * @deprecated since 5.0 because of package renaming
     * Use {@link com.hazelcast.persistence.BackupTaskState#isDone()} instead.
     */
    @Deprecated
    public boolean isDone() {
        return this == SUCCESS || this == FAILURE;
    }

    /**
     * @deprecated since 5.0 because of package renaming
     * Use {@link com.hazelcast.persistence.BackupTaskState#inProgress()} instead.
     */
    @Deprecated
    public boolean inProgress() {
        return this == NOT_STARTED || this == IN_PROGRESS;
    }
}
