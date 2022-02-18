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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.core.processor.DiagnosticProcessors;

import javax.annotation.Nonnull;
import java.util.function.Function;

/**
 * See {@link DiagnosticProcessors#writeLoggerP(FunctionEx)}.
 */
public class WriteLoggerP<T> extends AbstractProcessor {

    private Function<T, ? extends CharSequence> toStringFn;

    public WriteLoggerP(Function<T, ? extends CharSequence> toStringFn) {
        this.toStringFn = toStringFn;
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        getLogger().info(toStringFn.apply((T) item).toString());
        return true;
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        getLogger().fine(watermark.toString());
        return true;
    }
}
