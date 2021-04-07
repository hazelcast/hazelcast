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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.query.Predicate;
import com.hazelcast.sql.impl.extract.QueryPath;
import org.junit.Test;

import static com.hazelcast.jet.sql.impl.connector.map.QueryUtil.toPredicate;
import static org.assertj.core.api.Assertions.assertThat;

public class QueryUtilTest {

    @Test
    public void when_leftValueIsNull_then_returnsNull() {
        Predicate<Object, Object> predicate =
                toPredicate(new Object[]{null}, new int[]{0}, new int[]{0}, new QueryPath[]{QueryPath.KEY_PATH});

        assertThat(predicate).isNull();
    }
}
