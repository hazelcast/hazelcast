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

package com.hazelcast.jet.dag.tap;

import com.hazelcast.jet.container.ContainerDescriptor;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.data.DataReader;
import com.hazelcast.jet.data.tuple.JetTupleFactory;

import java.io.Serializable;

/**
 * Represents abstract source tap
 */
public interface SourceTap extends Serializable {

    /**
     * @return name of the tap
     */
    String getName();

    /**
     * Array of the input readers
     *
     * @param containerDescriptor descriptor of the corresponding container
     * @param vertex              corresponding vertex
     * @param tupleFactory        factory for the tuple creation
     * @return list of the input readers
     */
    DataReader[] getReaders(ContainerDescriptor containerDescriptor,
                            Vertex vertex,
                            JetTupleFactory tupleFactory);

}
