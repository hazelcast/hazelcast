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

package com.hazelcast.jet.pipeline.file;

import org.junit.Test;

import static com.hazelcast.jet.pipeline.file.WildcardMatcher.hasWildcard;
import static org.assertj.core.api.Assertions.assertThat;

public class WildcardMatcherTest {

    @Test
    public void shouldDetectWildcard() {
        assertThat(hasWildcard("/path/to/file/*")).isTrue();
        assertThat(hasWildcard("/path/to/file/?")).isTrue();
        assertThat(hasWildcard("/path/to/file/{abc}")).isTrue();
        assertThat(hasWildcard("/path/to/file/[abc]")).isTrue();
        assertThat(hasWildcard("*")).isTrue();

        assertThat(hasWildcard("/path/*/file/*")).isTrue();
        assertThat(hasWildcard("/path/\\*/file/*")).isTrue();
        assertThat(hasWildcard("/path/*/file/\\*")).isTrue();

        assertThat(hasWildcard("/path/to/file/\\*")).isFalse();
        assertThat(hasWildcard("\\*")).isFalse();
        assertThat(hasWildcard("/path/\\*/file/\\*")).isFalse();
    }
}
