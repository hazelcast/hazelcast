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

package com.hazelcast.jet.core;

import com.hazelcast.jet.config.JobConfig;

import javax.annotation.Nonnull;

/**
 * Description of the cause that has led to the job being suspended.
 * <p>
 * One reason for the job to be suspended is the user explicitly requesting it.
 * Another reason is encountering an error during execution, while being
 * configured to get suspended in such cases, instead of just failing. See
 * {@link JobConfig#setSuspendOnFailure(boolean)}.
 * <p>
 * When the job has been suspended due to an error, then the cause of that
 * error is provided.
 *
 * @since Jet 4.4
 */
public interface JobSuspensionCause {

    /**
     * True if the user explicitly suspended the job.
     */
    boolean requestedByUser();

    /**
     * True if Jet suspended the job because it encountered an error during
     * execution.
     */
    boolean dueToError();

    /**
     * Describes the error that has led to the suspension of the job. Throws
     * {@link UnsupportedOperationException} if job was not suspended due to an
     * error.
     */
    @Nonnull
    String errorCause();

    /**
     * Describes the job suspension cause in a human-readable form.
     */
    @Nonnull
    String description();

}
