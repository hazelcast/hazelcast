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

package com.hazelcast.internal.serialization.impl.compact;

import com.hazelcast.internal.serialization.impl.StreamSerializerAdapter;

import java.util.Objects;

/**
 * Differently than StreamSerializerAdapter this implementation checks if the input buffer should be returned to pool
 * or not according to returned object.
 */
public class CompactStreamSerializerAdapter extends StreamSerializerAdapter {

    public CompactStreamSerializerAdapter(CompactStreamSerializer compactStreamSerializer) {
        super(compactStreamSerializer);
    }

    @Override
    public String toString() {
        return "CompactStreamSerializerAdapter{serializer=" + serializer + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CompactStreamSerializerAdapter that = (CompactStreamSerializerAdapter) o;

        return Objects.equals(serializer, that.serializer);
    }

    @Override
    public int hashCode() {
        return serializer != null ? serializer.hashCode() : 0;
    }
}
