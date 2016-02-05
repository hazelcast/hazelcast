/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.actor;

import com.hazelcast.jet.spi.strategy.DataTransferringStrategy;

public final class ByReferenceDataTransferringStrategy implements DataTransferringStrategy<Object> {
    public static final DataTransferringStrategy<Object> INSTANCE = new ByReferenceDataTransferringStrategy();

    private ByReferenceDataTransferringStrategy() {
    }

    @Override
    public boolean byReference() {
        return true;
    }

    @Override
    public Object newInstance() {
        throw new IllegalStateException("Not supported");
    }

    @Override
    public void copy(Object sourceObject, Object targetObject) {
        throw new IllegalStateException("Not supported");
    }

    @Override
    public void clean(Object object) {
        throw new IllegalStateException("Not supported");
    }
}
