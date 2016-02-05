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

package com.hazelcast.jet.impl.data.io;

import com.hazelcast.jet.spi.strategy.DataTransferringStrategy;
import com.hazelcast.jet.impl.actor.ByReferenceDataTransferringStrategy;

public class DefaultObjectIOStream<T> extends AbstractIOStream<T> {
    public DefaultObjectIOStream(T[] buffer) {
        super(buffer, ByReferenceDataTransferringStrategy.INSTANCE);
    }

    public DefaultObjectIOStream(T[] buffer, DataTransferringStrategy dataTransferringStrategy) {
        super(buffer, dataTransferringStrategy);
    }
}
