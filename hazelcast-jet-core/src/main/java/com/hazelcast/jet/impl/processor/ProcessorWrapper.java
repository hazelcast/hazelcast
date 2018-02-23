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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Watermark;

import javax.annotation.Nonnull;

/**
 * Base class for processor wrappers. Delegates all calls to the wrapped
 * processor.
 */
public class ProcessorWrapper implements Processor {
    protected final Processor wrapped;

    protected ProcessorWrapper(Processor wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public boolean isCooperative() {
        return wrapped.isCooperative();
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        wrapped.init(outbox, context);
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        wrapped.process(ordinal, inbox);
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        return wrapped.tryProcessWatermark(watermark);
    }

    @Override
    public boolean tryProcess() {
        return wrapped.tryProcess();
    }

    @Override
    public boolean completeEdge(int ordinal) {
        return wrapped.completeEdge(ordinal);
    }

    @Override
    public boolean complete() {
        return wrapped.complete();
    }

    @Override
    public boolean saveToSnapshot() {
        return wrapped.saveToSnapshot();
    }

    @Override
    public void restoreFromSnapshot(@Nonnull Inbox inbox) {
        wrapped.restoreFromSnapshot(inbox);
    }

    @Override
    public boolean finishSnapshotRestore() {
        return wrapped.finishSnapshotRestore();
    }
}
