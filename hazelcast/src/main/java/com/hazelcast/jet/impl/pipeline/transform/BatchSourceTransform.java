/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.pipeline.transform;

import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.impl.ProcessorClassLoaderTLHolder;
import com.hazelcast.jet.impl.pipeline.PipelineImpl.Context;
import com.hazelcast.jet.impl.pipeline.Planner;
import com.hazelcast.jet.pipeline.BatchSource;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static com.hazelcast.jet.impl.util.Util.doWithClassLoader;
import static java.util.Collections.emptyList;

public class BatchSourceTransform<T> extends AbstractTransform implements BatchSource<T> {

    private static final long serialVersionUID = 1L;

    @Nonnull
    public ProcessorMetaSupplier metaSupplier;
    private boolean isAssignedToStage;

    public BatchSourceTransform(
            @Nonnull String name,
            @Nonnull ProcessorMetaSupplier metaSupplier
    ) {
        super(name, emptyList());
        this.metaSupplier = metaSupplier;
    }

    public void onAssignToStage() {
        if (isAssignedToStage) {
            throw new IllegalStateException("Sink " + name() + " was already assigned to a sink stage");
        }
        isAssignedToStage = true;
    }

    @Override
    public void addToDag(Planner p, Context context) {
        determineLocalParallelism(metaSupplier.preferredLocalParallelism(), context, false);
        p.addVertex(this, name(), determinedLocalParallelism(), metaSupplier);
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.writeObject(metaSupplier);
        out.writeBoolean(isAssignedToStage);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        metaSupplier = doWithClassLoader(ProcessorClassLoaderTLHolder.get(name()), () -> (ProcessorMetaSupplier) in.readObject());
        isAssignedToStage = in.readBoolean();
    }

}
