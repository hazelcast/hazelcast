/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.probing;

import com.hazelcast.internal.metrics.ProbeLevel;

/**
 * From a usability point of view the {@link ProbeRenderContext} is a bit
 * cumbersome and smells like over-abstraction. It is purely introduced to
 * achieve the goal of rendering without creating garbage objects. That means
 * state needs to be reused. This object is the place where state can be kept in
 * a way that allows reuse between rendering cycles.
 *
 * The {@link ProbeRenderer} itself usually changes for each cycle as it tends
 * to be dependent on output stream objects handed to it.
 */
public interface ProbeRenderContext {

    /**
     * Causes a {@link ProbingCycle} that is directed at the given
     * {@link ProbeRenderer}.
     *
     * This method does not support multi-threading. If potentially concurrent calls
     * to this method should be made each should originate from its own
     * {@link ProbeRenderContext}.
     *
     * @param renderer not null; is called for each active prove with a key and
     *        value to convert them to the renderer specific format.
     */
    void render(ProbeLevel level, ProbeRenderer renderer);
}
