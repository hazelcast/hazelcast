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

/**
 * Thrown when a named job is submitted while there is an <em>active job</em>
 * with the same name. Job is <em>active</em> if it is running, suspended or
 * waiting to be run, until it completes or fails.
 *
 * @since Jet 3.0
 */
public class JobAlreadyExistsException extends JetException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates the exception
     */
    public JobAlreadyExistsException() {
    }

    /**
     * Creates the exception with a message.
     */
    public JobAlreadyExistsException(String message) {
        super(message);
    }

    /**
     * Creates the exception with a message and a cause.
     */
    public JobAlreadyExistsException(String message, Throwable cause) {
        super(message, cause);
    }
}
