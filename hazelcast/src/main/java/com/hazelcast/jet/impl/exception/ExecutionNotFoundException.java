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

package com.hazelcast.jet.impl.exception;

import com.hazelcast.jet.JetException;
import com.hazelcast.spi.exception.SilentException;

import static com.hazelcast.jet.Util.idToString;

/**
 * Thrown in response to operations sent from the coordinator, if the
 * target doesn't know the execution.
 */
public class ExecutionNotFoundException extends JetException implements SilentException {

    private static final long serialVersionUID = 1L;

    public ExecutionNotFoundException() {
    }

    public ExecutionNotFoundException(long executionId) {
        this("Execution " + idToString(executionId) + " not found");
    }

    public ExecutionNotFoundException(String message) {
        super(message);
    }

    public ExecutionNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public ExecutionNotFoundException(Throwable cause) {
        super(cause);
    }
}
