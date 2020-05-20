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

package com.hazelcast.sql.impl.calcite;

import com.hazelcast.sql.impl.QueryUtils;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableResolver;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Test table resolver.
 */
public class TestTableResolver implements TableResolver {

    private final String searchPath;
    private final Collection<Table> tables;

    public static TestTableResolver create(Table... tables) {
        return create(null, tables);
    }

    public static TestTableResolver create(String searchPath, Table... tables) {
        return new TestTableResolver(searchPath, Arrays.asList(tables));
    }

    private TestTableResolver(String searchPath, Collection<Table> tables) {
        this.searchPath = searchPath;
        this.tables = tables;
    }

    @Override
    public List<List<String>> getDefaultSearchPaths() {
        if (searchPath == null) {
            return null;
        } else {
            return Collections.singletonList(Arrays.asList(QueryUtils.CATALOG, searchPath));
        }
    }

    @Override
    public Collection<Table> getTables() {
        return tables;
    }
}
