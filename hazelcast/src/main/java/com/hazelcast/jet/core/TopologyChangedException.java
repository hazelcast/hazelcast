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

import com.hazelcast.jet.JetException;

/**
 * Thrown when a topology change causes a job to abort. The job
 * is aborted when a participating member leaves the cluster. If
 * auto-restart is enabled for the job, the job will be restarted
 * automatically and this exception will not be thrown to the user.
 *
 * @since Jet 3.0
 */
public class TopologyChangedException extends JetException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates the exception
     */
    public TopologyChangedException() {
    }

    /**
     * Creates the exception with a message.
     */
    public TopologyChangedException(String message) {
        super(message);
    }

    /**
     * Creates the exception with a message and a cause.
     */
    public TopologyChangedException(String message, Throwable cause) {
        super(message, cause);
    }
}
