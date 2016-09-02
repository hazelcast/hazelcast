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

package com.hazelcast.jet.impl.runtime;

import com.hazelcast.jet.Edge;
import com.hazelcast.jet.impl.actor.Actor;
import com.hazelcast.jet.impl.actor.ComposedActor;
import com.hazelcast.jet.impl.runtime.task.VertexTask;
import java.util.ArrayList;
import java.util.List;

public class DataChannel {
    private final Edge edge;
    private final boolean isShuffled;
    private final List<ComposedActor> actors;
    private final VertexRunner source;
    private final VertexRunner target;

    public DataChannel(VertexRunner source,
                       VertexRunner target,
                       Edge edge) {
        this.edge = edge;

        this.source = source;
        this.target = target;

        this.isShuffled = !edge.isLocal();
        this.actors = new ArrayList<>();

        init();
    }

    private void init() {
        for (VertexTask vertexTask : this.source.getVertexTasks()) {
            this.actors.add(vertexTask.registerOutputChannel(this, this.edge, this.target));
        }
    }

    public List<ComposedActor> getActors() {
        return this.actors;
    }

    public boolean isShuffled() {
        return this.isShuffled;
    }

    public void close() {
        for (Actor actor : getActors()) {
            actor.handleProducerCompleted();
        }
    }
}
