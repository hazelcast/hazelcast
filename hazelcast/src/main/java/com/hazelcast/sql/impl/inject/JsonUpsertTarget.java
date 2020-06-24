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

package com.hazelcast.sql.impl.inject;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.math.BigDecimal;
import java.math.BigInteger;

public class JsonUpsertTarget implements UpsertTarget {

    private StringBuilder json;
    private int i;

    JsonUpsertTarget() {
    }

    @Override
    public UpsertInjector createInjector(String path) {
        return value -> {
            // TODO:
            if (i > 0) {
                json.append(',');
            }

            json.append('"').append(path).append('"');

            json.append(':');

            if (value == null) {
                json.append("null");
            } else if ((value instanceof Number && !(value instanceof BigDecimal) && !(value instanceof BigInteger))
                    || value instanceof Boolean) {
                json.append(QueryDataType.VARCHAR.convert(value));
            } else {
                json.append('"').append(QueryDataType.VARCHAR.convert(value)).append('"');
            }

            i++;
        };
    }

    @Override
    public void init() {
        json = new StringBuilder("{");
        i = 0;
    }

    @Override
    public Object conclude() {
        json.append("}");

        StringBuilder json = this.json;
        this.json = null;
        i = 0;
        return new HazelcastJsonValue(json.toString());
    }
}
