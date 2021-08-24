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
 * Use {@link com.hazelcast.persistence.BackupTaskStatus} instead.
 */
@Deprecated
public class BackupTaskStatus {

    private final BackupTaskState state;
    private final int completed;
    private final int total;

    @Deprecated
    public BackupTaskStatus(BackupTaskState state, int completed, int total) {
        this.state = state;
        this.completed = completed;
        this.total = total;
    }

    @Deprecated
    public BackupTaskState getState() {
        return state;
    }

    @Deprecated
    public int getCompleted() {
        return completed;
    }

    @Deprecated
    public int getTotal() {
        return total;
    }

    @Deprecated
    public float getProgress() {
        return total > 0 ? (float) completed / total : 0;
    }

    @Override
    public String toString() {
        return "BackupTaskStatus{state=" + state + ", completed=" + completed + ", total=" + total + '}';
    }

    @Override
    @SuppressWarnings("checkstyle:innerassignment")
    public boolean equals(Object obj) {
        final BackupTaskStatus that;
        return obj instanceof BackupTaskStatus
                && this.completed == (that = (BackupTaskStatus) obj).completed
                && this.total == that.total
                && this.state == that.state;
    }

    @Override
    public int hashCode() {
        int result = state != null ? state.hashCode() : 0;
        result = 31 * result + completed;
        result = 31 * result + total;
        return result;
    }
}
