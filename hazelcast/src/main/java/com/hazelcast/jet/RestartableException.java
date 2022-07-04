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

package com.hazelcast.jet;

import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;

/**
 * Exception to throw from job-executing methods to indicate a failure that can
 * be resolved by restarting the job.
 * <p>
 * It is handled when thrown from:<ul>
 *     <li>{@link Processor} methods
 *     <li>lambda functions: key & timestamp extractors, map functions...
 *     <li>sources/sinks
 *     <li>{@code init()} method in {@link Processor}, {@link
 *          ProcessorSupplier} and {@link ProcessorMetaSupplier} (but not their
 *          {@code close()} methods)
 *     <li>...
 * </ul>
 *
 * If this exception is caught, the job will be terminated abruptly and
 * restarted (if {@link JobConfig#setAutoScaling so configured}).
 *
 * @since Jet 3.0
 */
public class RestartableException extends JetException {

    private static final long serialVersionUID = 1L;

    public RestartableException() {
    }

    public RestartableException(String message) {
        super(message);
    }

    public RestartableException(String message, Throwable cause) {
        super(message, cause);
    }

    public RestartableException(Throwable cause) {
        super(cause);
    }

}
