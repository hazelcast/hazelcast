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

package com.hazelcast.test;

public class DefaultTaskProgress implements TaskProgress {
    private final long timestamp = System.currentTimeMillis();
    private final int total;
    private final int replicated;
    private final float progress;
    private final boolean completed;

    public DefaultTaskProgress(int total, int replicated) {
        this.total = total;
        this.replicated = replicated;
        this.progress = ((float) replicated) / total;
        this.completed = total == replicated;
    }

    @Override
    public boolean isCompleted() {
        return completed;
    }

    @Override
    public float progress() {
        return progress;
    }

    @Override
    public long timestamp() {
        return timestamp;
    }

    @Override
    public String getProgressString() {
        return new StringBuilder()
                .append(replicated).append("/").append(total).append("=").append(progress * 100).append("%")
                .toString();
    }
}
