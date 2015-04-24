/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.metrics.renderers;
/**
 * A comma separated key/value {@link ProbeRenderer} implementation.
 *
 * The output will be something like:
 * <code>
 *    {time=123,a=1,b=2,....}
 * </code>
 *
 * This renderer is very useful for automated analysis of the performance log files.
 */
public class CommaSeparatedKeyValueProbeRenderer implements ProbeRenderer {

    private final StringBuilder sb = new StringBuilder();

    @Override
    public void start() {
        sb.setLength(0);
        sb.append("{").append("time").append("=").append(System.currentTimeMillis());
    }

    @Override
    public void renderLong(String name, long value) {
        sb.append(',').append(name).append('=').append(value);
    }

    @Override
    public void renderDouble(String name, double value) {
        sb.append(',').append(name).append('=').append(value);
    }

    @Override
    public void renderException(String name, Exception e) {
        sb.append(',').append(name).append('=').append("NA");
    }

    @Override
    public void renderNoValue(String name) {
        sb.append(',').append(name).append('=').append("NA");
    }

    @Override
    public void finish() {
        sb.append("}\n");
    }

    @Override
    public String getResult() {
        return sb.toString();
    }
}
