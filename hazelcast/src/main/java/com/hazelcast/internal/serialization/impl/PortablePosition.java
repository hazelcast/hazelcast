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

import com.hazelcast.nio.serialization.FieldType;

import java.util.List;

interface PortablePosition {
    // used for all field types
    int getStreamPosition();

    int getIndex();

    // used for portables only
    boolean isNull();

    int getLen();

    int getFactoryId();

    int getClassId();

    // determines type of position
    boolean isMultiPosition();

    boolean isEmpty();

    // convenience
    boolean isNullOrEmpty();

    boolean isLast();

    boolean isAny();

    List<PortablePosition> asMultiPosition();

    FieldType getType();
}
