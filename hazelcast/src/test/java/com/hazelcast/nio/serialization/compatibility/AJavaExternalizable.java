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

package com.hazelcast.nio.serialization.compatibility;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class AJavaExternalizable implements Externalizable {

    int i;
    float f;

    public AJavaExternalizable() {
    }

    public AJavaExternalizable(int anInt, float aFloat) {
        i = anInt;
        f = aFloat;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(i);
        out.writeFloat(f);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        i = in.readInt();
        f = in.readFloat();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AJavaExternalizable that = (AJavaExternalizable) o;

        if (i != that.i) {
            return false;
        }
        return Float.compare(that.f, f) == 0;
    }

    @Override
    public int hashCode() {
        int result = i;
        result = 31 * result + (f != +0.0f ? Float.floatToIntBits(f) : 0);
        return result;
    }

    @Override
    public String toString() {
        return "AJavaExternalizable";
    }
}
