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

package com.hazelcast.projection.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.impl.Extractable;

import java.io.IOException;
import java.util.Objects;

import static com.hazelcast.internal.util.Preconditions.checkFalse;
import static com.hazelcast.internal.util.Preconditions.checkHasText;

/**
 * Projection that extracts the values of the given attribute and returns it.
 * <p>
 * The attributePath does not support the [any] operator.
 * The input object has to be an instance of Extractable in order for the projection to work.
 *
 * @param <I> type of the input
 */
public final class SingleAttributeProjection<I, O> implements Projection<I, O>, IdentifiedDataSerializable {

    private String attributePath;

    SingleAttributeProjection() {
    }

    public SingleAttributeProjection(String attributePath) {
        checkHasText(attributePath, "attributePath must not be null or empty");
        checkFalse(attributePath.contains("[any]"), "attributePath must not contain [any] operators");
        this.attributePath = attributePath;
    }

    @Override
    @SuppressWarnings("unchecked")
    public O transform(I input) {
        if (input instanceof Extractable) {
            return (O) ((Extractable) input).getAttributeValue(attributePath);
        }
        throw new IllegalArgumentException("The given map entry is not extractable");
    }

    @Override
    public int getFactoryId() {
        return ProjectionDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ProjectionDataSerializerHook.SINGLE_ATTRIBUTE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(attributePath);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        attributePath = in.readString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SingleAttributeProjection<?, ?> that = (SingleAttributeProjection<?, ?>) o;
        return Objects.equals(attributePath, that.attributePath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(attributePath);
    }
}
