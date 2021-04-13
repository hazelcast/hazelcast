/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class Timers {

    private static volatile Timers INSTANCE = new Timers(true);

    private final List<AbstractTimer> allTimers = new ArrayList<>();
    private final boolean useNoop;

    public final AbstractTimer executionPlan_initialize = add("executionPlan_initialize");
    public final AbstractTimer submitCooperativeTasklets = add("submitCooperativeTasklets");
    public final AbstractTimer submitLightJobOperation_run = add("submitLightJobOperation_run");
    public final AbstractTimer noopPInitToClose = add("noopPInitToClose");
    public final AbstractTimer jobExecService_runLightJob_synchronization_inner = add("jobExecService_synchronization_inner");
    public final AbstractTimer jobExecService_runLightJob_synchronization_outer = add("jobExecService_synchronization_outer");
    public final AbstractTimer jobExecService_runLightJob_verifyClusterInfo = add("jobExecService_runLightJob_verifyClusterInfo");
    public final AbstractTimer jobExecService_completeExecution = add("jobExecService_completeExecution");
    public final AbstractTimer initExecOp_deserializePlan = add("initExecOp_deserializePlan");
    public final AbstractTimer execCtx_initialize = add("execCtx_initialize");
    public final AbstractTimer lightMasterContext_start = add("lightMasterContext_start");
    public final AbstractTimer execPlanBuilder_createPlans = add("execPlanBuilder_createPlans");
    public final AbstractTimer lightMasterContext_serializeOnePlan = add("lightMasterContext_serializeOnePlan");
    public final AbstractTimer initResponseTime = add("initResponseTime");
    public final AbstractTimer init = add("init");
    public final AbstractTimer processorClose = add("processorClose");

    private long globalStart = System.nanoTime();

    public Timers(boolean useNoop) {
        this.useNoop = useNoop;
    }

    public static void reset(boolean useNoop) {
        INSTANCE = new Timers(useNoop);
    }

    public void setGlobalStart() {
        globalStart = System.nanoTime();
    }

    private AbstractTimer add(String name) {
        AbstractTimer t = useNoop ? new NoopTimer() : new Timer(name);
        allTimers.add(t);
        return t;
    }

    public void printAll() {
        if (useNoop) {
            return;
        }

        System.out.println("-- sorted by start");
        allTimers.sort(Comparator.comparing(t -> ((Timer) t).totalTimeToStart));
        for (AbstractTimer t : allTimers) {
            ((Timer) t).print();
        }
        System.out.println("-- sorted by end");
        allTimers.sort(Comparator.comparing(t -> ((Timer) t).totalTimeToEnd));
        for (AbstractTimer t : allTimers) {
            ((Timer) t).print();
        }
    }

    public static Timers i() {
        return INSTANCE;
    }

    private static final AtomicLongFieldUpdater<Timer> TOTAL_TIME_TO_START_UPDATER =
            AtomicLongFieldUpdater.newUpdater(Timer.class, "totalTimeToStart");
    private static final AtomicLongFieldUpdater<Timer> TOTAL_TIME_TO_END_UPDATER =
            AtomicLongFieldUpdater.newUpdater(Timer.class, "totalTimeToEnd");
    private static final AtomicIntegerFieldUpdater<Timer> RUN_COUNT_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(Timer.class, "runCount");

    public interface AbstractTimer {
        void start();
        void stop();
    }

    private final class Timer implements AbstractTimer {
        private final String name;
        volatile long totalTimeToStart;
        volatile long totalTimeToEnd;
        volatile int runCount;

        public Timer(String name) {
            this.name = name;
        }

        @Override
        public void start() {
            TOTAL_TIME_TO_START_UPDATER.addAndGet(this, System.nanoTime() - globalStart);
            RUN_COUNT_UPDATER.incrementAndGet(this);
        }

        @Override
        public void stop() {
            TOTAL_TIME_TO_END_UPDATER.addAndGet(this, System.nanoTime() - globalStart);
        }

        private void print() {
            System.out.printf("%50s: toStart: %8s, toEnd: %8s, dur: %8s, runCount: %d\n",
                    name,
                    runCount != 0 ? NANOSECONDS.toMicros(totalTimeToStart / runCount) : "--",
                    runCount != 0 ? NANOSECONDS.toMicros(totalTimeToEnd / runCount) : "--",
                    runCount != 0 ? NANOSECONDS.toMicros((totalTimeToEnd - totalTimeToStart) / runCount) : "--",
                    runCount);
        }
    }

    private static final class NoopTimer implements AbstractTimer {
        @Override
        public void start() { }

        @Override
        public void stop() { }
    }
}
