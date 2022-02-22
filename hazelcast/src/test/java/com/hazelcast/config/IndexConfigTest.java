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

import com.hazelcast.config.BitmapIndexOptions.UniqueKeyTransformation;
import com.hazelcast.query.QueryConstants;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IndexConfigTest {

    @Test
    public void testIndexDefaults() {
        IndexConfig config = new IndexConfig();

        assertEquals(IndexType.SORTED, config.getType());
        assertNull(config.getName());
        assertEquals(QueryConstants.KEY_ATTRIBUTE_NAME.value(), config.getBitmapIndexOptions().getUniqueKey());
        assertEquals(UniqueKeyTransformation.OBJECT, config.getBitmapIndexOptions().getUniqueKeyTransformation());
    }

    @Test
    public void testIndexEquality() {
        checkIndexQuality(new IndexConfig(), new IndexConfig(), true);

        checkIndexQuality(new IndexConfig(IndexType.SORTED), new IndexConfig(IndexType.SORTED), true);
        checkIndexQuality(new IndexConfig(IndexType.HASH), new IndexConfig(IndexType.HASH), true);
        checkIndexQuality(new IndexConfig(IndexType.HASH), new IndexConfig(IndexType.SORTED), false);
        checkIndexQuality(new IndexConfig(IndexType.BITMAP), new IndexConfig(IndexType.BITMAP), true);
        checkIndexQuality(new IndexConfig(IndexType.BITMAP), new IndexConfig(IndexType.HASH), false);

        IndexConfig actual = new IndexConfig(IndexType.BITMAP);
        actual.getBitmapIndexOptions().setUniqueKey("a");
        checkIndexQuality(new IndexConfig(IndexType.BITMAP), actual, false);

        actual = new IndexConfig(IndexType.BITMAP);
        actual.getBitmapIndexOptions().setUniqueKeyTransformation(UniqueKeyTransformation.RAW);
        checkIndexQuality(new IndexConfig(IndexType.BITMAP), actual, false);

        checkIndexQuality(new IndexConfig().setName("name"), new IndexConfig().setName("name"), true);
        checkIndexQuality(new IndexConfig().setName("name"), new IndexConfig().setName("name2"), false);
    }

    private void checkIndexQuality(IndexConfig config1, IndexConfig config2, boolean expected) {
        assertEquals(expected, config1.equals(config2));
        assertEquals(expected, config1.hashCode() == config2.hashCode());

        config1.addAttribute("col1");
        config2.addAttribute("col1");
        assertEquals(expected, config1.equals(config2));
        assertEquals(expected, config1.hashCode() == config2.hashCode());

        config1.addAttribute("col2");
        config2.addAttribute("col2");
        assertEquals(expected, config1.equals(config2));
        assertEquals(expected, config1.hashCode() == config2.hashCode());

        List<String> cols = new LinkedList<>();
        cols.add("col1");
        cols.add("col2");
        config2.setAttributes(cols);
        assertEquals(expected, config1.equals(config2));
        assertEquals(expected, config1.hashCode() == config2.hashCode());

        config2.addAttribute("col3");
        assertNotEquals(config1, config2);
        assertNotEquals(config1.hashCode(), config2.hashCode());
    }

    @Test(expected = NullPointerException.class)
    public void testTypeNull() {
        new IndexConfig().setType(null);
    }

    @Test(expected = NullPointerException.class)
    public void testAttributesNull() {
        new IndexConfig().setAttributes(null);
    }

    @Test(expected = NullPointerException.class)
    public void testAttributeNull() {
        new IndexConfig().setAttributes(Collections.singletonList(null));
    }

    @Test(expected = NullPointerException.class)
    public void testAttributeNullAdd() {
        new IndexConfig().addAttribute(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAttributeEmpty() {
        new IndexConfig().setAttributes(Collections.singletonList(""));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAttributeEmptyAdd() {
        new IndexConfig().addAttribute("");
    }

}
