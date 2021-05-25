/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl.extract;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.sql.impl.extract.QueryExtractor;
import com.hazelcast.sql.impl.extract.QueryTarget;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.Arrays;
import java.util.List;

@NotThreadSafe
public class JournalQueryTarget implements QueryTarget {
    private static final List<String> FIELDS = Arrays.asList("key", "type", "oldValue", "newValue");

    private Object[] target;

    private final QueryTargetDescriptor keyTargetDescriptor;
    private final QueryTargetDescriptor valueTargetDescriptor;

    public JournalQueryTarget(
            QueryTargetDescriptor keyTargetDescriptor,
            QueryTargetDescriptor valueTargetDescriptor
    ) {
        this.keyTargetDescriptor = keyTargetDescriptor;
        this.valueTargetDescriptor = valueTargetDescriptor;
    }

    @Override
    public void setTarget(Object target, Data targetData) {
        assert targetData == null;
        this.target = (Object[]) target;
        assert this.target.length == FIELDS.size();
    }

    @Override
    public QueryExtractor createExtractor(String path, QueryDataType type) {
        if (path == null) {
            return () -> target;
        }
        int index = FIELDS.indexOf(path);
        if (index == -1) {
            throw new IllegalArgumentException(path);
        }
        return () -> type.convert(target[index]);
    }
}
