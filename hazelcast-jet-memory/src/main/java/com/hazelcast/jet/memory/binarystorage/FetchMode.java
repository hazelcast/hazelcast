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

package com.hazelcast.jet.memory.binarystorage;

/**
 * Specifies the semantics of a fetch operation on binary storage.
 */
public enum FetchMode {
    /** Get an existing entry, take no action if absent. */
    JUST_GET,
    /** Create an entry if absent. */
    CREATE_IF_ABSENT,
    /** Always append a new value to a multivalue entry, creating the entry as needed. */
    CREATE_OR_APPEND
}
