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

/**
 * This class is for Non-java clients. Please do not remove or modify.
 */

package com.hazelcast.client.test;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

/**
 * This class is for Non-java clients. Please do not remove or modify.
 */
public class PortableFactory implements com.hazelcast.nio.serialization.PortableFactory {
    public static final int FACTORY_ID = 666;

    class SampleRunnableTask implements Portable, Runnable {

        private String name;

        SampleRunnableTask() {
        }

        public void run() {
            System.out.println("Running " + name);
        }

        public int getFactoryId() {
            return PortableFactory.FACTORY_ID;
        }

        public int getClassId() {
            return 1;
        }

        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeString("n", name);
        }

        public void readPortable(PortableReader reader) throws IOException {
            name = reader.readString("n");
        }
    }

    class BasePortable implements Portable {
        @Override
        public int getFactoryId() {
            return PortableFactory.FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return 3;
        }

        @Override
        public void writePortable(PortableWriter portableWriter)
                throws IOException {
        }

        @Override
        public void readPortable(PortableReader portableReader)
                throws IOException {
        }
    }

    class Derived1Portable extends BasePortable {
        @Override
        public int getClassId() {
            return 4;
        }
    }

    class Derived2Portable extends BasePortable {
        @Override
        public int getClassId() {
            return 5;
        }
    }

    @Override
    public Portable create(int classId) {
        if (classId == 1) {
            return new SampleRunnableTask();
        } else if (classId == 2) {
            return new Employee();
        } else if (classId == 3) {
            return new BasePortable();
        } else if (classId == 4) {
            return new Derived1Portable();
        } else if (classId == 5) {
            return new Derived2Portable();
        } else if (classId == 6) {
            return new Student();
        }
        return null;
    }
}
