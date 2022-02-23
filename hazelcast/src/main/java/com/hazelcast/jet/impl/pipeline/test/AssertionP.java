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

package com.hazelcast.jet.impl.pipeline.test;

import com.hazelcast.function.BiConsumerEx;
import com.hazelcast.function.ConsumerEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;

import javax.annotation.Nonnull;
import java.util.concurrent.TimeUnit;

public final class AssertionP<S, T> extends AbstractProcessor {

    private static final long TIMER_INTERVAL = TimeUnit.MILLISECONDS.toNanos(200);

    private final SupplierEx<? extends S> createFn;
    private final BiConsumerEx<? super S, ? super T> receiveFn;
    private final ConsumerEx<? super S> timerFn;
    private final ConsumerEx<? super S> completeFn;

    private S state;
    private long nextTimerSchedule;

    private AssertionP(SupplierEx<? extends S> createFn,
                       BiConsumerEx<? super S, ? super T> receiveFn,
                       ConsumerEx<? super S> timerFn,
                       ConsumerEx<? super S> completeFn
    ) {
        this.createFn = createFn;
        this.receiveFn = receiveFn;
        this.timerFn = timerFn;
        this.completeFn = completeFn;
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        super.init(context);
        state = createFn.get();
    }

    @Override
    public boolean tryProcess() {
        maybeFireTimer();
        return true;
    }

    private void maybeFireTimer() {
        long now = System.nanoTime();
        if (nextTimerSchedule == 0 || now >= nextTimerSchedule) {
            timerFn.accept(state);
            nextTimerSchedule = now + TIMER_INTERVAL;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        receiveFn.accept(state, (T) item);
        maybeFireTimer();
        return true;
    }

    @Override
    public boolean complete() {
        completeFn.accept(state);
        return true;
    }

    @Nonnull
    public static <A, T> ProcessorMetaSupplier assertionP(
        @Nonnull String name,
        @Nonnull SupplierEx<? extends A> createFn,
        @Nonnull BiConsumerEx<? super A, ? super T> receiveFn,
        @Nonnull ConsumerEx<? super A> timerFn,
        @Nonnull ConsumerEx<? super A> completeFn
    ) {
        return ProcessorMetaSupplier.forceTotalParallelismOne(ProcessorSupplier.of(
            () -> new AssertionP<>(createFn, receiveFn, timerFn, completeFn)), name
        );
    }
}
