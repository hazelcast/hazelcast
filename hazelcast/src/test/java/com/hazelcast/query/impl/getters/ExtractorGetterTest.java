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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.query.extractor.ValueExtractor;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ExtractorGetterTest {

    private static final InternalSerializationService UNUSED = null;

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Test
    public void isCacheable() {
        // GIVEN
        ExtractorGetter getter = new ExtractorGetter(UNUSED, mock(ValueExtractor.class), "argument");

        // THEN
        assertThat(getter.isCacheable(), is(true));
    }

    @Test
    public void getReturnType() {
        // GIVEN
        ExtractorGetter getter = new ExtractorGetter(UNUSED, mock(ValueExtractor.class), "argument");

        // EXPECT
        expected.expect(UnsupportedOperationException.class);

        // WHEN
        getter.getReturnType();
    }
}
