/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.internal.util.concurrent.update.ConcurrentConveyor;
import com.hazelcast.internal.util.concurrent.update.Pipe;
import com.hazelcast.jet.Punctuation;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.jet.impl.util.ProgressTracker;
import com.hazelcast.jet.impl.util.SkewReductionPolicy;
import com.hazelcast.util.function.Predicate;

import java.util.Collection;

import static com.hazelcast.jet.impl.execution.DoneItem.DONE_ITEM;

/**
 * {@link InboundEdgeStream} implemented in terms of a {@link ConcurrentConveyor}.
 * The conveyor has as many 1-to-1 concurrent queues as there are upstream tasklets
 * contributing to it.
 */
public class ConcurrentInboundEdgeStream implements InboundEdgeStream {

    private final int ordinal;
    private final int priority;
    private final ConcurrentConveyor<Object> conveyor;
    private final ProgressTracker tracker = new ProgressTracker();
    private final PunctuationDetector puncDetector = new PunctuationDetector();
    private long lastEmittedPunc = Long.MIN_VALUE;
    private final SkewReductionPolicy skewReductionPolicy;

    public ConcurrentInboundEdgeStream(ConcurrentConveyor<Object> conveyor, int ordinal, int priority,
            long maxSkew, long priorityDrainingThreshold, boolean forceAdvancePunctuation) {
        this.conveyor = conveyor;
        this.ordinal = ordinal;
        this.priority = priority;
        this.skewReductionPolicy = new SkewReductionPolicy(
                conveyor.queueCount(), maxSkew, priorityDrainingThreshold, forceAdvancePunctuation);
    }

    @Override
    public int ordinal() {
        return ordinal;
    }

    @Override
    public int priority() {
        return priority;
    }

    /**
     * Drains the inbound queues into the {@code dest} collection. Some queues
     * may be skipped, as decided by the {@link SkewReductionPolicy}.
     */
    @Override
    public ProgressState drainTo(Collection<Object> dest) {
        tracker.reset();
        for (int drainOrderIdx = 0; drainOrderIdx < conveyor.queueCount(); drainOrderIdx++) {
            int queueIndex = skewReductionPolicy.toQueueIndex(drainOrderIdx);
            if (skewReductionPolicy.shouldStopDraining(queueIndex, tracker.isMadeProgress())) {
                break;
            }
            final Pipe<Object> q = conveyor.queue(queueIndex);
            if (q == null) {
                continue;
            }
            Punctuation punc = drainUpToPunc(q, dest);
            if (puncDetector.isDone) {
                conveyor.removeQueue(queueIndex);
                continue;
            }
            if (punc != null && skewReductionPolicy.observePunc(queueIndex, punc.timestamp())) {
                break;
            }
        }

        long bottomPunct = skewReductionPolicy.bottomObservedPunc();
        if (bottomPunct > lastEmittedPunc) {
            dest.add(new Punctuation(bottomPunct));
            lastEmittedPunc = bottomPunct;
        }

        return tracker.toProgressState();
    }

    /**
     * Drains the supplied queue into a {@code dest} collection, up to the next
     * {@link Punctuation}. Also updates the {@code tracker} with new status.
     *
     * @return the drained punctuation, if any; {@code null} otherwise
     */
    private Punctuation drainUpToPunc(Pipe<Object> queue, Collection<Object> dest) {
        puncDetector.reset(dest);

        int drainedCount = queue.drain(puncDetector);
        tracker.mergeWith(ProgressState.valueOf(drainedCount > 0, puncDetector.isDone));

        puncDetector.dest = null;
        return puncDetector.punc;
    }

    /**
     * Drains a concurrent conveyor's queue while watching for {@link Punctuation}s.
     * When encountering a punctuation, prevents draining more items.
     */
    private static final class PunctuationDetector implements Predicate<Object> {
        Collection<Object> dest;
        Punctuation punc;
        boolean isDone;

        void reset(Collection<Object> newDest) {
            dest = newDest;
            punc = null;
            isDone = false;
        }

        @Override
        public boolean test(Object o) {
            if (o instanceof Punctuation) {
                assert punc == null : "Received multiple Punctuations without a call to reset()";
                punc = (Punctuation) o;
                return false;
            }
            if (o == DONE_ITEM) {
                isDone = true;
                return false;
            }
            dest.add(o);
            return true;
        }
    }
}
