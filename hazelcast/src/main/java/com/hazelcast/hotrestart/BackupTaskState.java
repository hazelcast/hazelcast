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

package com.hazelcast.hotrestart;

import com.hazelcast.internal.probing.CodedEnum;

/**
 * The state of the hot restart backup task
 */
public enum BackupTaskState implements CodedEnum {
    /** No backup task has yet been run */
    NO_TASK(0),
    /** The backup task has been submitted but not yet started. */
    NOT_STARTED(1),
    /** The backup task is currently in progress */
    IN_PROGRESS(2),
    /** The backup task has failed */
    FAILURE(3),
    /** The backup task completed successfully */
    SUCCESS(4);

    private final int code;

    BackupTaskState(int code) {
        this.code = code;
    }

    /** Returns true if the backup task completed (successfully or with failure) */
    public boolean isDone() {
        return this == SUCCESS || this == FAILURE;
    }

    /** Returns if the backup task is in progress. The task could also not be started yet but will be. */
    public boolean inProgress() {
        return this == NOT_STARTED || this == IN_PROGRESS;
    }

    @Override
    public int getCode() {
        return code;
    }
}
