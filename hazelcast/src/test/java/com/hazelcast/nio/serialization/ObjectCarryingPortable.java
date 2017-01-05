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

package com.hazelcast.nio.serialization;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class ObjectCarryingPortable implements Portable {

    private Object object;

    public ObjectCarryingPortable() {
    }

    public ObjectCarryingPortable(Object object) {
        this.object = object;
    }

    @Override
    public int getFactoryId() {
        return TestSerializationConstants.PORTABLE_FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return TestSerializationConstants.OBJECT_CARRYING_PORTABLE;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        ObjectDataOutput output = writer.getRawDataOutput();
        output.writeObject(object);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        ObjectDataInput input = reader.getRawDataInput();
        object = input.readObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ObjectCarryingPortable that = (ObjectCarryingPortable) o;

        if (object != null ? !object.equals(that.object) : that.object != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return object != null ? object.hashCode() : 0;
    }
}
