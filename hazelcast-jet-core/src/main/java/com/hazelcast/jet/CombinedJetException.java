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

package com.hazelcast.jet;

import java.io.PrintStream;
import java.util.List;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Collects exception from different nodes
 */
public class CombinedJetException extends RuntimeException {
    private final List<Throwable> errors;

    /**
     * Create a combined JetException with multiple errors
     * @param errors list of errors
     */
    public CombinedJetException(List<Throwable> errors) {
        checkNotNull(errors);
        this.errors = errors;
    }

    /**
     * Returns combined list of errors
     *
     * @return combined list of errors
     */
    public List<Throwable> getErrors() {
        return errors;
    }

    /**
     * Return first exception or null if there are no exceptions
     *
     * @return first exception or null if there are no exceptions
     */
    public Throwable getCause() {
        return errors.size() > 0 ? errors.get(0) : null;
    }

    /**
     * Print stack trace of all the exceptions
     * @param s {@code PrintStream} to write exceptions to
     */
    public void printStackTrace(PrintStream s) {
        for (Throwable error : errors) {
            s.println("====== Exception ============");
            error.printStackTrace(s);
            s.println("====== Done      ==============");
        }
    }

}
