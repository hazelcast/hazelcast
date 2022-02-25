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

package com.hazelcast.client.standalone.model;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

public class MyPortableElement implements Portable {
    public static final int FACTORY_ID = 1;
    public static final int CLASS_ID = 1;

    private int id;
    private Long date;

    private MyPortableElement() {
    }

    public MyPortableElement(int id) {
        this.id = id;
        this.date = 123L;
    }

    @Override
    public int getFactoryId() {
        return FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return CLASS_ID;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeInt("id", id);
        writer.writeLong("date", date);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        id = reader.readInt("id");
        date = reader.readLong("date");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MyPortableElement that = (MyPortableElement) o;

        if (id != that.id) {
            return false;
        }
        return date != null ? date.equals(that.date) : that.date == null;

    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + (date != null ? date.hashCode() : 0);
        return result;
    }

    public static class Factory implements PortableFactory {
        @Override
        public Portable create(int classId) {
            if (classId == CLASS_ID) {
                return new MyPortableElement();
            } else {
                throw new IllegalArgumentException("Unknown class ID " + classId);
            }
        }
    }
}
