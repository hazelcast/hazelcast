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

package com.hazelcast.jet.dag.source;

import com.hazelcast.jet.container.ContainerContext;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.impl.actor.Producer;

import java.io.Serializable;

/**
 * Represents abstract source
 */
public interface Source extends Serializable {

    /**
     * @return name of the source
     */
    String getName();

    /**
     * Array of the input producers
     *
     * @param containerContext descriptor of the corresponding container
     * @param vertex              corresponding vertex
     * @return list of the input readers
     */
    Producer[] getProducers(ContainerContext containerContext, Vertex vertex);

}
