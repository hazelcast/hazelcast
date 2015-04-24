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
 * A human friendly {@link ProbeRenderer} implementation.
 *
 * The output will be something like:
 * <code>
 *     a=1
 *     b=2
 * </code>
 */
public class HumanFriendlyProbeRenderer implements ProbeRenderer {

    private final StringBuilder sb = new StringBuilder();

    @Override
    public void start() {
        sb.setLength(0);
    }

    @Override
    public void renderLong(String name, long value) {
        sb.append(name).append('=').append(value).append('\n');
    }

    @Override
    public void renderDouble(String name, double value) {
        sb.append(name).append('=').append(value).append('\n');
    }

    @Override
    public void renderException(String name, Exception e) {
        sb.append(name).append('=').append(e.getMessage()).append('\n');
    }

    @Override
    public void renderNoValue(String name) {
        sb.append(name).append('=').append("NA").append('\n');
    }

    @Override
    public void finish() {
        //no-op
    }

    @Override
    public String getResult() {
        return sb.toString();
    }
}
