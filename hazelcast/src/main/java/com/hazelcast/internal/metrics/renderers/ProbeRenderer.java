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
 * With the ProbeRenderer the content of the {@link com.hazelcast.internal.metrics.MetricsRegistry} can be rendered.
 */
public interface ProbeRenderer {

    void start();

    void renderLong(String name, long value);

    void renderDouble(String name, double value);

    void renderException(String name, Exception e);

    void renderNoValue(String name);

    void finish();

    String getResult();
}
