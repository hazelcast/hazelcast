/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl;

import com.hazelcast.core.ExecutionCallback;

/**
 * A ExecutionCallback implementation that simplifies the implementation of the ExecutionCallback by only
 * needing to implement a single method.
 *
 * @param <E>
 */
public abstract class SimpleExecutionCallback<E> implements ExecutionCallback<E> {

    public abstract void notify(Object response);

    @Override
    public final void onResponse(E response) {
        notify(response);
    }

    @Override
    public final void onFailure(Throwable t) {
        notify(t);
    }
}
