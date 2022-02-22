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

package com.hazelcast.internal.json;

import com.hazelcast.core.HazelcastException;

import java.io.IOException;

/**
 * This class is a placeholder for Json object/arrays that are not
 * parsed by {@link com.hazelcast.query.impl.getters.AbstractJsonGetter}.
 * It is used to distinguish between query paths that do not exist and
 * existing ones that does not terminate with either of `true`, `false`,
 * `null`, `number` or `string`.
 */
public class NonTerminalJsonValue extends JsonValue {

    public static final NonTerminalJsonValue INSTANCE = new NonTerminalJsonValue();

    @Override
    void write(JsonWriter writer) throws IOException {
        throw new HazelcastException("This object must not be encoded");
    }
}
