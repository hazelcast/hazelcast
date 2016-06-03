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
import com.hazelcast.jet.data.DataWriter;
import com.hazelcast.spi.NodeEngine;

/**
 * Abstract class which represents any sink tap;
 */
public abstract class SinkTap implements Tap {
    /**
     * Return writers for the corresponding tap;
     *
     * @param nodeEngine          - Hazelcast nodeEngine;
     * @param containerDescriptor - descriptor of the container;
     * @return - list of the data writers;
     */
    public abstract DataWriter[] getWriters(NodeEngine nodeEngine,
                                            ContainerDescriptor containerDescriptor);

    public boolean isSource() {
        return false;
    }

    public boolean isSink() {
        return true;
    }

    public abstract SinkTapWriteStrategy getTapStrategy();

    public abstract SinkOutputStream getSinkOutputStream();

    public boolean isPartitioned() {
        return true;
    }
}
