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

package com.hazelcast.collection.impl.queue.model;

import com.hazelcast.internal.util.Preconditions;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Objects;

public class VersionedObject<T> implements Serializable {
    private static final long serialVersionUID = 1L;
    private T object;
    private int version;

    public VersionedObject(@Nonnull T object) {
        this(object, -1);
    }

    public VersionedObject(@Nonnull T object, int version) {
        Preconditions.checkNotNull(object);
        this.object = object;
        this.version = version;
    }

    public T getObject() {
        return object;
    }

    public void setObject(T object) {
        this.object = object;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        VersionedObject<?> that = (VersionedObject<?>) o;
        return version == that.version
                && object.equals(that.object);
    }

    @Override
    public int hashCode() {
        return Objects.hash(object, version);
    }

    @Override
    public String toString() {
        return "VersionedObject{"
                + "object=" + object
                + ", version=" + version
                + '}';
    }
}
