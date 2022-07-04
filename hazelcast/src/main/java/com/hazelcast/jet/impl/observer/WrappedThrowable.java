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

package com.hazelcast.jet.impl.observer;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Objects;

/**
 * A class used when a Throwable is added by Jet to a collection or stream of
 * user objects and we want to safely distinguish Jet's throwable from user's,
 * however rare that is.
 */
public class WrappedThrowable implements IdentifiedDataSerializable {

    private Throwable throwable;

    WrappedThrowable() { //needed for deserialization
    }

    private WrappedThrowable(Throwable throwable) {
        this.throwable = throwable;
    }

    @Nonnull
    public static WrappedThrowable of(@Nonnull Throwable throwable) {
        return new WrappedThrowable(Objects.requireNonNull(throwable));
    }

    public Throwable get() {
        return throwable;
    }

    @Override
    public int getFactoryId() {
        return JetObserverDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return JetObserverDataSerializerHook.WRAPPED_THROWABLE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(throwable);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        throwable = in.readObject();
    }
}
