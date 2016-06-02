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

package com.hazelcast.jet.io.internal.impl;

import com.hazelcast.jet.io.api.IOContext;
import com.hazelcast.jet.io.api.ObjectReader;
import com.hazelcast.jet.io.api.ObjectReaderFactory;

public class DefaultObjectReaderFactory implements ObjectReaderFactory {
    private final IOContext ioContext;

    public DefaultObjectReaderFactory(IOContext ioContext) {
        this.ioContext = ioContext;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> ObjectReader<T> getReader(byte typeID) {
        return this.ioContext.getDataType(typeID).getObjectReader();
    }
}
