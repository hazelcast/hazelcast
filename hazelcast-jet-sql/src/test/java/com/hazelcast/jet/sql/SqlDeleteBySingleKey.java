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

package com.hazelcast.jet.sql;

import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlResult;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SqlDeleteBySingleKey extends SqlTestSupport {
    @BeforeClass
    public static void setUpClass() {
        initialize(2, null);
    }

    @Test
    public void delete() throws Exception {
        IMap<Integer, Integer> map = instance().getMap("test_map");
        map.put(1, 1);

        SqlResult execute = instance().getSql().execute("delete from test_map where __key = 1");
        assertThat(execute.updateCount()).isEqualTo(1);
    }
}
