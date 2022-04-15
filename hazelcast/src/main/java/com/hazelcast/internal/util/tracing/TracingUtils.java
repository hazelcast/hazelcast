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

package com.hazelcast.internal.util.tracing;

import com.hazelcast.internal.nio.ClassLoaderUtil;

public class TracingUtils {
    private static final TracingContext TRACING_CONTEXT;

    static {
        TracingContext tracingContext = null;
        try {
            Class<?> asyncProfilerContext =
                    ClassLoaderUtil.tryLoadClass("com.hazelcast.profiler.AsyncProfilerTracingContext");
            tracingContext = (TracingContext) asyncProfilerContext.newInstance();
        } catch (Exception e) {
            e.printStackTrace();
            tracingContext = new NullObjectTracingContext();
        }
        TRACING_CONTEXT = tracingContext;
    }

    public static TracingContext context() {
        return TRACING_CONTEXT;
    }
}
