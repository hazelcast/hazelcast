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

package com.hazelcast.test;

public class DefaultTaskProgress implements TaskProgress {
    private final long timestamp = System.currentTimeMillis();
    private final int total;
    private final int done;
    private final double progress;
    private final boolean completed;

    public DefaultTaskProgress(int total, int done) {
        this.total = total;
        this.done = done;
        this.progress = ((double) done) / total;
        this.completed = total == done;
    }

    @Override
    public boolean isCompleted() {
        return completed;
    }

    @Override
    public double progress() {
        return progress;
    }

    @Override
    public long timestamp() {
        return timestamp;
    }

    @Override
    public String getProgressString() {
        return String.format("%d/%d=%.2f%%", done, total, progress * 100);
    }
}
