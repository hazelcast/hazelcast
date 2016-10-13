/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet2.impl;

/**
 * Javadoc pending.
 */
public class ProgressTracker {
    private boolean isMadeProgress;
    private boolean isDone = true;

    public void reset() {
        isMadeProgress = false;
        isDone = true;
    }

    public void madeProgress(boolean isMadeProgress) {
        this.isMadeProgress |= isMadeProgress;
    }

    public void madeProgress() {
        madeProgress(true);
    }

    /**
     * Equivalent to {@code update(NO_PROGRESS)}, but more descriptive in certain usages.
     */
    public void notDone() {
        isDone = false;
    }

    public boolean isDone() {
        return isDone;
    }

    public ProgressState toProgressState() {
        return ProgressState.valueOf(isMadeProgress, isDone);
    }

    @Override
    public String toString() {
        return toProgressState().toString();
    }
}
