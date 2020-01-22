/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl.getters;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.internal.serialization.impl.NavigableJsonInputAdapter;
import com.hazelcast.internal.serialization.impl.StringNavigableJsonAdapter;
import com.hazelcast.query.QueryException;

import java.io.IOException;

public final class JsonGetter extends AbstractJsonGetter {

    public static final JsonGetter INSTANCE = new JsonGetter();

    private JsonFactory factory = new JsonFactory();

    protected JsonGetter() {
        super(null);
    }

    @Override
    protected NavigableJsonInputAdapter annotate(Object object) {
        HazelcastJsonValue hazelcastJson = (HazelcastJsonValue) object;
        return new StringNavigableJsonAdapter(hazelcastJson.toString(), 0);
    }

    @Override
    JsonParser createParser(Object obj) throws IOException {
        if (obj instanceof HazelcastJsonValue) {
            return factory.createParser(obj.toString());
        } else {
            throw new QueryException("Queried object is not of HazelcastJsonValue type. It is " + obj.getClass());
        }
    }
}
