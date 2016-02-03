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

package com.hazelcast.internal.monitors;

/**
 * A writer like structure dedicated for the {@link PerformanceMonitorPlugin} rendering.
 */
public abstract class PerformanceLogWriter {

    protected final StringBuilder sb = new StringBuilder();

    public abstract void startSection(String name);

    public abstract void endSection();

    public abstract void writeEntry(String s);

    public abstract void writeKeyValueEntry(String key, String value);

    public abstract void writeKeyValueEntry(String key, double value);

    public abstract void writeKeyValueEntry(String key, long value);

    public abstract void writeKeyValueEntry(String key, boolean value);

    abstract void write(PerformanceMonitorPlugin plugin);

    protected void clean() {
        sb.setLength(0);
    }

    public int length() {
        return sb.length();
    }

    public void copyInto(char[] target) {
        sb.getChars(0, sb.length(), target, 0);
    }
}
