/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.util.ConstructorFunction;

public class ArrayPortableFactory implements PortableFactory {

    private final ConstructorFunction<Integer, Portable>[] constructors;
    private final int len;

    public ArrayPortableFactory(ConstructorFunction<Integer, Portable>[] ctorArray) {
        if (ctorArray != null && ctorArray.length > 0) {
            len = ctorArray.length;
            constructors = new ConstructorFunction[len];
            System.arraycopy(ctorArray, 0, constructors, 0, len);
        } else {
            len = 0;
            constructors = new ConstructorFunction[len];
        }
    }

    public Portable create(int classId) {
        if (classId >= 0 && classId < len) {
            ConstructorFunction<Integer, Portable> factory = constructors[classId];
            return factory != null ? factory.createNew(classId) : null;
        }
        return null;
    }
}
