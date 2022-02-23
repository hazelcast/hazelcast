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

package com.hazelcast.config;

import com.hazelcast.internal.config.IndexConfigReadOnly;
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
        return new IndexConfigReadOnly(new IndexConfig());
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
        getConfig().setAttributes(Collections.singletonList("column"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void addColumn1() {
        getConfig().addAttribute("column");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void addColumn2() {
        getConfig().addAttribute("column");
    }
}
