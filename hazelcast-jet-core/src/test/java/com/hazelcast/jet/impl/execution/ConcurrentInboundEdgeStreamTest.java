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

import com.hazelcast.jet.impl.util.ProgressState;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class ConcurrentInboundEdgeStreamTest {

    @Test
    public void when_twoEmittersOneDoneFirst_then_madeProgress() {
        IntegerSequenceEmitter emitter1 = new IntegerSequenceEmitter(1, 2, 1);
        IntegerSequenceEmitter emitter2 = new IntegerSequenceEmitter(6, 1, 2);

        ConcurrentInboundEdgeStream stream = new ConcurrentInboundEdgeStream(
                new IntegerSequenceEmitter[]{emitter1, emitter2}, 0, 0);

        ArrayList<Object> list = new ArrayList<>();
        ProgressState progressState = stream.drainTo(list);

        // emitter1 returned 1 and 2; emitter2 returned 6
        // emitter1 is now done
        assertEquals(Arrays.asList(1, 2, 6), list);
        assertEquals(ProgressState.MADE_PROGRESS, progressState);

        list.clear();
        progressState = stream.drainTo(list);
        // emitter2 returned 7 and now both emitters are done
        assertEquals(Collections.singletonList(7), list);
        assertEquals(ProgressState.DONE, progressState);

        // both emitters are now done and made no progress since last call
        list.clear();
        progressState = stream.drainTo(list);
        assertEquals(0, list.size());
        assertEquals(ProgressState.WAS_ALREADY_DONE, progressState);
    }

    @Test
    public void when_twoEmittersDrainedAtOnce_then_firstCallDone() {
        IntegerSequenceEmitter emitter1 = new IntegerSequenceEmitter(1, 2, 1);
        IntegerSequenceEmitter emitter2 = new IntegerSequenceEmitter(6, 1, 1);

        ConcurrentInboundEdgeStream stream = new ConcurrentInboundEdgeStream(
                new IntegerSequenceEmitter[]{emitter1, emitter2}, 0, 0);

        ArrayList<Object> list = new ArrayList<>();
        ProgressState progressState = stream.drainTo(list);

        // emitter1 returned 1 and 2; emitter2 returned 6
        // both are now done
        assertEquals(Arrays.asList(1, 2, 6), list);
        assertEquals(ProgressState.DONE, progressState);
    }

    @Test
    public void when_allEmittersInitiallyDone_then_firstCallDone() {
        IntegerSequenceEmitter emitter1 = new IntegerSequenceEmitter(1, 2, 0);
        IntegerSequenceEmitter emitter2 = new IntegerSequenceEmitter(6, 1, 0);

        ConcurrentInboundEdgeStream stream = new ConcurrentInboundEdgeStream(
                new IntegerSequenceEmitter[]{emitter1, emitter2}, 0, 0);

        ArrayList<Object> list = new ArrayList<>();
        ProgressState progressState = stream.drainTo(list);

        assertEquals(0, list.size());
        assertEquals(ProgressState.WAS_ALREADY_DONE, progressState);

        list.clear();
        progressState = stream.drainTo(list);
        assertEquals(0, list.size());
        assertEquals(ProgressState.WAS_ALREADY_DONE, progressState);
    }

    @Test
    public void when_oneEmitterWithNoProgress_then_noProgress() {
        NoProgressEmitter emitter1 = new NoProgressEmitter();
        IntegerSequenceEmitter emitter2 = new IntegerSequenceEmitter(1, 1, 1);

        ConcurrentInboundEdgeStream stream = new ConcurrentInboundEdgeStream(
                new InboundEmitter[]{emitter1, emitter2}, 0, 0);

        ArrayList<Object> list = new ArrayList<>();
        ProgressState progressState = stream.drainTo(list);

        assertEquals(Collections.singletonList(1), list);
        assertEquals(ProgressState.MADE_PROGRESS, progressState);
        // now emitter2 is done, emitter1 is not but has no progress
        list.clear();
        progressState = stream.drainTo(list);
        assertEquals(0, list.size());
        assertEquals(ProgressState.NO_PROGRESS, progressState);

        // now make emitter1 done, without returning anything
        emitter1.done = true;

        list.clear();
        progressState = stream.drainTo(list);
        assertEquals(0, list.size());
        assertEquals(ProgressState.WAS_ALREADY_DONE, progressState);
    }

    private static final class IntegerSequenceEmitter implements InboundEmitter {

        private int currentValue;
        private int drainSize;
        private int endValue;

        /**
         * @param startAt Value to start at
         * @param drainSize Number of items to emit in one {@code drainTo} call
         * @param drainCallsUntilDone Total number of {@code drainTo} Calls until done
         */
        IntegerSequenceEmitter(int startAt, int drainSize, int drainCallsUntilDone) {
            currentValue = startAt;
            this.drainSize = drainSize;
            this.endValue = startAt + drainCallsUntilDone * drainSize;
        }

        @Override
        public ProgressState drainTo(Collection<Object> dest) {
            int i;
            for (i = 0; i < drainSize && currentValue < endValue; i++, currentValue++)
                dest.add(currentValue);

            return ProgressState.valueOf(i > 0, currentValue >= endValue);
        }
    }

    private static final class NoProgressEmitter implements InboundEmitter {

        boolean done;

        @Override
        public ProgressState drainTo(Collection<Object> dest) {
            return done ? ProgressState.WAS_ALREADY_DONE : ProgressState.NO_PROGRESS;
        }
    }

}