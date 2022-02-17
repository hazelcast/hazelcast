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

package com.hazelcast.query.impl.getters;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SuffixModifierUtilsTest {

    @Test
    public void removeModifierSuffix_whenSuffixIsNotPresent_thenReturnInputString() {
        String attribute = "foo";
        String result = SuffixModifierUtils.removeModifierSuffix(attribute);

        assertSame(attribute, result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void removeModifierSuffix_whenMultipleOpeningTokensArePresent_thenThrowIllegalArgumentException() {
        String attribute = "foo[[";
        SuffixModifierUtils.removeModifierSuffix(attribute);
    }

    @Test(expected = IllegalArgumentException.class)
    public void removeModifierSuffix_whenNoClosingTokenIsPresent_thenThrowIllegalArgumentException() {
        String attribute = "foo[*";
        SuffixModifierUtils.removeModifierSuffix(attribute);
    }

    @Test
    public void removeModifierSuffix_whenSuffixIsPresent_thenReturnStringWithoutSuffix() {
        String attributeWithModifier = "foo[*]";
        String result = SuffixModifierUtils.removeModifierSuffix(attributeWithModifier);

        assertEquals("foo", result);
    }
}
