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

abstract class BinaryClassDefinition implements ClassDefinition {

    int factoryId;
    int classId;
    int version = -1;

    private transient byte[] binary;

    public BinaryClassDefinition() {
    }

    public final int getFactoryId() {
        return factoryId;
    }

    public final int getClassId() {
        return classId;
    }

    public final int getVersion() {
        return version;
    }

    public final byte[] getBinary() {
        return binary;
    }

    final void setBinary(byte[] binary) {
        this.binary = binary;
    }
}
