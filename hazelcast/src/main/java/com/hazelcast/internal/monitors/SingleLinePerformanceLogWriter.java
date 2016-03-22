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

import static com.hazelcast.util.StringUtil.LINE_SEPARATOR;

/**
 * A {@link PerformanceLogWriter} that writes using a single line. Useful for automatic analysis e.g. using grep.
 */
class SingleLinePerformanceLogWriter extends PerformanceLogWriter {

    private boolean firstEntry = true;

    @Override
    public void startSection(String name) {
        appendComma();
        sb.append(name).append('[');
        firstEntry = true;
    }

    @Override
    public void endSection() {
        sb.append(']');
    }

    @Override
    public void writeEntry(String s) {
        appendComma();
        sb.append(s);
    }

    private void appendComma() {
        if (firstEntry) {
            firstEntry = false;
        } else {
            sb.append(',');
        }
    }

    @Override
    public void writeKeyValueEntry(String key, String value) {
        appendComma();
        sb.append(key).append('=').append(value);
    }

    @Override
    public void writeKeyValueEntry(String key, double value) {
        appendComma();
        sb.append(key).append('=').append(value);
    }

    @Override
    public void writeKeyValueEntry(String key, long value) {
        appendComma();
        sb.append(key).append('=').append(value);
    }

    @Override
    public void writeKeyValueEntry(String key, boolean value) {
        appendComma();
        sb.append(key).append('=').append(value);
    }

    @Override
    protected void clean() {
        firstEntry = true;
        super.clean();
    }

    @Override
    void write(PerformanceMonitorPlugin plugin) {
        clean();

        sb.append(System.currentTimeMillis());
        sb.append(' ');
        plugin.run(this);
        sb.append(LINE_SEPARATOR);
    }
}
