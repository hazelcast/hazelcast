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

package com.hazelcast.json.internal;

import com.hazelcast.internal.json.JsonObject;

/**
 * JsonSerializable is a serialization interface that serializes/de-serializes
 * to/from JSON.
 */
public interface JsonSerializable {

    /**
     * Serializes state represented by this object into a {@link JsonObject}.
     *
     * @return the JSON representation of this object
     */
    JsonObject toJson();

    /**
     * Extracts the state from the given {@code json} object and mutates the
     * state of this object.
     *
     * @param json the JSON object carrying state for this object
     */
    void fromJson(JsonObject json);
}
