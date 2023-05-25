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

package com.hazelcast.jet;

import com.hazelcast.jet.core.JobStatus;

import java.util.UUID;

/**
 * Invoked upon job status change. <ol>
 * <li> This listener observes that the status is transitioned to {@link
 *      JobStatus#SUSPENDED SUSPENDED}/{@link JobStatus#FAILED FAILED} upon job
 *      {@linkplain Job#suspend suspension}/{@linkplain Job#cancel termination},
 *      {@link JobStatus#NOT_RUNNING NOT_RUNNING} upon job {@linkplain
 *      Job#resume resume}/{@linkplain Job#restart restart}, and {@link
 *      JobStatus#SUSPENDED_EXPORTING_SNAPSHOT SUSPENDED_EXPORTING_SNAPSHOT}
 *      upon {@linkplain Job#exportSnapshot exporting snapshot}, all of which
 *      are reported as user-initiated. Other transitions, such as from {@link
 *      JobStatus#NOT_RUNNING NOT_RUNNING} to {@link JobStatus#STARTING
 *      STARTING} or from {@link JobStatus#STARTING STARTING} to {@link
 *      JobStatus#RUNNING RUNNING}, are not reported as user-initiated.
 * <li> This listener is not notified for {@link JobStatus#COMPLETING
 *      COMPLETING} status and observes that the status is transitioned from
 *      {@link JobStatus#RUNNING RUNNING} to {@link JobStatus#FAILED FAILED}
 *      upon job {@linkplain Job#cancel termination}.
 * <li> This listener is automatically deregistered after a {@linkplain
 *      JobStatus#isTerminal terminal event}. </ol>
 *
 * @see Job#addStatusListener(JobStatusListener)
 * @see Job#removeStatusListener(UUID)
 * @since 5.3
 */
@FunctionalInterface
public interface JobStatusListener {
    /**
     * Invoked upon job status change. <ol>
     * <li> This listener observes that the status is transitioned to {@link
     *      JobStatus#SUSPENDED SUSPENDED}/{@link JobStatus#FAILED FAILED} upon job
     *      {@linkplain Job#suspend suspension}/{@linkplain Job#cancel termination},
     *      {@link JobStatus#NOT_RUNNING NOT_RUNNING} upon job {@linkplain
     *      Job#resume resume}/{@linkplain Job#restart restart}, and {@link
     *      JobStatus#SUSPENDED_EXPORTING_SNAPSHOT SUSPENDED_EXPORTING_SNAPSHOT}
     *      upon {@linkplain Job#exportSnapshot exporting snapshot}, all of which
     *      are reported as user-initiated. Other transitions, such as from {@link
     *      JobStatus#NOT_RUNNING NOT_RUNNING} to {@link JobStatus#STARTING
     *      STARTING} or from {@link JobStatus#STARTING STARTING} to {@link
     *      JobStatus#RUNNING RUNNING}, are not reported as user-initiated.
     * <li> This listener is not notified for {@link JobStatus#COMPLETING
     *      COMPLETING} status and observes that the status is transitioned from
     *      {@link JobStatus#RUNNING RUNNING} to {@link JobStatus#FAILED FAILED}
     *      upon job {@linkplain Job#cancel termination}.
     * <li> This listener is automatically deregistered after a {@linkplain
     *      JobStatus#isTerminal terminal event}. </ol>
     *
     * @param event Holds information about the previous and new job statuses,
     *        reason for the status change, and whether it is user-initiated.
     */
    void jobStatusChanged(JobStatusEvent event);
}
