/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.logging;

import java.util.logging.Level;


/**
 * Abstract {@link ILogger} implementation that provides implementations for the convenience methods like
 * finest,info,warning and severe.
 */
public abstract class AbstractLogger implements ILogger {

    public void finest(String message) {
        log(Level.FINEST, message);
    }

    public void finest(String message, Throwable thrown) {
        log(Level.FINEST, message, thrown);
    }

    public void finest(Throwable thrown) {
        log(Level.FINEST, thrown.getMessage(), thrown);
    }

    public boolean isFinestEnabled() {
        return isLoggable(Level.FINEST);
    }

    public void info(String message) {
        log(Level.INFO, message);
    }

    public void severe(String message) {
        log(Level.SEVERE, message);
    }

    public void severe(Throwable thrown) {
        log(Level.SEVERE, thrown.getMessage(), thrown);
    }

    public void severe(String message, Throwable thrown) {
        log(Level.SEVERE, message, thrown);
    }

    public void warning(String message) {
        log(Level.WARNING, message);
    }

    public void warning(Throwable thrown) {
        log(Level.WARNING, thrown.getMessage(), thrown);
    }

    public void warning(String message, Throwable thrown) {
        log(Level.WARNING, message, thrown);
    }
}
