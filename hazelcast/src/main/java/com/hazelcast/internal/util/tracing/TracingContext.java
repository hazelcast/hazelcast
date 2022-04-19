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

import java.io.Closeable;

/**
 * This class is an abstraction over passing context correlation id to all the tools that needs it.
 * The default implementation that will be loaded in runtime is {@link NullObjectTracingContext}.
 * All the methods there are empty. This class is an abstract class, not the interface, to exploit
 * the internals of JIT class hierarchy analysis (CHA), which created faster compilation for an
 * abstract class with exactly one implementation. Inlining + CHA + NullObject pattern should
 * make absolutely no overhead for the following methods.
 */
public abstract class TracingContext implements Closeable {
    public abstract void setCorrelationId(String correlationId);
    public abstract void clearCorrelationId();
    public abstract String getCorrelationId();
}
