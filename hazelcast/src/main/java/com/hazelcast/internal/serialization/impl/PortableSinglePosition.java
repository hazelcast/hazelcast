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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.FieldType;

import java.util.List;

class PortableSinglePosition implements PortablePosition {

    // used for all positions
    FieldDefinition fd;
    //    AccessType accessType;
    int position;

    // poison pills to indicate null-pointer or empty-array
    boolean nil;

    // used for arrays only
    int index = -1;
    int len = -1;

    // used for portables only
    int factoryId = -1;
    int classId = -1;

    boolean last;
    boolean any;

    @Override
    public int getStreamPosition() {
        return position;
    }

    @Override
    public int getIndex() {
        return index;
    }

    @Override
    public boolean isNull() {
        return nil;
    }

    @Override
    public int getLen() {
        return len;
    }

    @Override
    public boolean isEmpty() {
        return len == 0;
    }

    @Override
    public boolean isNullOrEmpty() {
        return isNull() || isEmpty();
    }

    @Override
    public boolean isLast() {
        return last;
    }

    @Override
    public boolean isAny() {
        return any;
    }

    @Override
    public int getFactoryId() {
        return factoryId;
    }

    @Override
    public int getClassId() {
        return classId;
    }

    @Override
    public boolean isMultiPosition() {
        return false;
    }

    @Override
    public List<PortablePosition> asMultiPosition() {
        throw new RuntimeException("Not a multi-position!");
    }

    @Override
    public FieldType getType() {
        if (fd != null) {
            return fd.getType();
        }
        return null;
    }

    public void reset() {
        fd = null;
        position = 0;
        nil = false;
        int index = -1;
        int len = -1;
        int factoryId = -1;
        int classId = -1;
        last = false;
    }
}
