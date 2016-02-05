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

package com.hazelcast.jet.impl.container;

import java.util.List;
import java.util.ArrayList;

import com.hazelcast.jet.spi.dag.Edge;
import com.hazelcast.jet.api.actor.ObjectActor;
import com.hazelcast.jet.api.actor.ComposedActor;
import com.hazelcast.jet.api.container.DataChannel;
import com.hazelcast.jet.api.container.ContainerTask;
import com.hazelcast.jet.api.container.ProcessingContainer;

public class DefaultDataChannel implements DataChannel {
    private final Edge edge;
    private final boolean isShuffled;
    private final List<ComposedActor> actors;
    private final ProcessingContainer sourceContainer;
    private final ProcessingContainer targetContainer;

    public DefaultDataChannel(ProcessingContainer sourceContainer,
                              ProcessingContainer targetContainer,
                              Edge edge) {
        this.edge = edge;

        this.sourceContainer = sourceContainer;
        this.targetContainer = targetContainer;

        this.isShuffled = edge.isShuffled();
        this.actors = new ArrayList<ComposedActor>();

        init();
    }

    private void init() {
        for (ContainerTask containerTask : this.sourceContainer.getContainerTasks()) {
            this.actors.add(containerTask.registerOutputChannel(this, this.edge, this.targetContainer));
        }
    }

    @Override
    public List<ComposedActor> getActors() {
        return this.actors;
    }

    @Override
    public ProcessingContainer getSourceContainer() {
        return this.sourceContainer;
    }

    @Override
    public ProcessingContainer getTargetContainer() {
        return this.targetContainer;
    }

    @Override
    public boolean isShuffled() {
        return this.isShuffled;
    }

    @Override
    public void close() {
        for (ObjectActor actor : getActors()) {
            actor.handleProducerCompleted();
        }
    }
}
