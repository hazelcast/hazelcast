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

package com.hazelcast.jet.impl.util;

/**
 * Tracks the overall progress and completion state of a multi-pipeline operation.
 * <ul><li>
 *     The operation as a whole made progress if any pipeline made progress.
 * </li><li>
 *     The operation as a whole is done if each pipeline is done.
 * </li></ul>
 * The initial state of the progress tracker is "done without progress"
 * (equivalent to {@link ProgressState#WAS_ALREADY_DONE}).
 */
public class ProgressTracker {
    private boolean isMadeProgress;
    private boolean isDone = true;

    /**
     * Resets the progress tracker to the initial state of "done without progress".
     */
    public void reset() {
        isMadeProgress = false;
        isDone = true;
    }

    /**
     * Lets this progress tracker know whether some pipeline made progress.
     */
    public void madeProgress(boolean isMadeProgress) {
        this.isMadeProgress |= isMadeProgress;
    }

    /**
     * Lets this progress tracker know that some pipeline made progress.
     */
    public void madeProgress() {
        madeProgress(true);
    }

    public boolean isMadeProgress() {
        return isMadeProgress;
    }

    /**
     * Merges the state of this progress tracker with the given {@code ProgressState} enum
     * member.
     */
    public void mergeWith(ProgressState state) {
        isMadeProgress = isMadeProgress || state.isMadeProgress();
        isDone = isDone && state.isDone();
    }

    /**
     * Equivalent to {@code mergeWith(NO_PROGRESS)}, but more descriptive in certain usages.
     */
    public void notDone() {
        isDone = false;
    }

    /**
     * Marks the progress as done.
     */
    public void done() {
        isDone = true;
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
