/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IndexConfigReadOnlyTest {

    private static IndexConfig getConfig() {
        return new IndexConfig().getAsReadOnly();
    }

    private static IndexColumnConfig getColumnConfig() {
        return new IndexColumnConfig().getAsReadOnly();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setType() {
        getConfig().setType(IndexType.SORTED);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setName() {
        getConfig().setName("index");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setColumns() {
        getConfig().setColumns(Collections.singletonList(new IndexColumnConfig("column")));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void addColumn1() {
        getConfig().addColumn("column");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void addColumn2() {
        getConfig().addColumn(new IndexColumnConfig("column"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setColumnName() {
        getColumnConfig().setName("column");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setColumnAscending() {
        getColumnConfig().setAscending(true);
    }
}
