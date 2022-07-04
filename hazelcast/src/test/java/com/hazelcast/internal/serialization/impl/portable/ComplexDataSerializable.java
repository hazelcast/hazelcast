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

package com.hazelcast.internal.serialization.impl.portable;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.Portable;

import java.io.IOException;

@SuppressWarnings("unused")
class ComplexDataSerializable implements DataSerializable {

    private DataSerializable ds;
    private Portable portable;
    private DataSerializable ds2;

    ComplexDataSerializable() {
    }

    ComplexDataSerializable(Portable portable, DataSerializable ds, DataSerializable ds2) {
        this.portable = portable;
        this.ds = ds;
        this.ds2 = ds2;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(ds);
        out.writeObject(portable);
        out.writeObject(ds2);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        ds = in.readObject();
        portable = in.readObject();
        ds2 = in.readObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ComplexDataSerializable that = (ComplexDataSerializable) o;
        if (ds != null ? !ds.equals(that.ds) : that.ds != null) {
            return false;
        }
        if (ds2 != null ? !ds2.equals(that.ds2) : that.ds2 != null) {
            return false;
        }
        if (portable != null ? !portable.equals(that.portable) : that.portable != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = ds != null ? ds.hashCode() : 0;
        result = 31 * result + (portable != null ? portable.hashCode() : 0);
        result = 31 * result + (ds2 != null ? ds2.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ComplexDataSerializable{" + "ds=" + ds
                + ", portable=" + portable
                + ", ds2=" + ds2
                + '}';
    }
}
