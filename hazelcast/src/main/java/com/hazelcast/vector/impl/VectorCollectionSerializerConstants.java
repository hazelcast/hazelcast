/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.impl;

abstract class VectorCollectionSerializerConstants {

    public static final int FACTORY_ID = -100;

    public static final short VECTOR_DOCUMENT = 1;
    public static final short DATA_VECTOR_DOCUMENT = 2;
    public static final short SEARCH_OPTIONS = 3;
    public static final short DATA_SEARCH_RESULT = 4;
    public static final short SINGLE_VECTOR_VALUES = 15;
    public static final short MULTIPLE_VECTOR_VALUES = 16;
}
