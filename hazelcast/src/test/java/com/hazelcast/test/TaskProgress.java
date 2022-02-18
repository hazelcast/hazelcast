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

/**
 * Interface of classes that hold progress information of asserted conditions
 * in tests. Instances of this interface are meant to be created at each
 * iteration of eventual completion checks.
 *
 * @see ProgressCheckerTask
 * @see HazelcastTestSupport#assertCompletesEventually(ProgressCheckerTask, long)
 */
public interface TaskProgress {

    /**
     * Tells whether the checked task completed or not
     *
     * @return {@code true} if the task completed, {@code false} otherwise
     */
    boolean isCompleted();

    /**
     * Returns the current progress as a fractional number between 0 and 1.
     *
     * @return the progress
     */
    double progress();

    /**
     * Returns the timestamp when this progress snapshot was taken
     *
     * @return the timestamp of the progress snapshot
     */
    long timestamp();

    /**
     * Returns a formatted progress string with the available information.
     * Called when the available time for the task to complete exceeded.
     *
     * @return the formatted progress string
     */
    String getProgressString();
}
