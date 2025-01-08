/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SearchOptionsTest {

    @Test
    void defaults() {
        var defaultOptions = SearchOptions.builder().build();
        assertThat(defaultOptions.getLimit()).isEqualTo(10);
        assertThat(defaultOptions.isIncludeValue()).isFalse();
        assertThat(defaultOptions.isIncludeVectors()).isFalse();
        assertThat(defaultOptions.getHints()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 0})
    void limitValidationBuilder(int limit) {
        assertThatThrownBy(() -> SearchOptions.builder().limit(limit).build())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 0})
    void limitValidationFactoryMethod(int limit) {
        assertThatThrownBy(() -> SearchOptions.of(limit, false, false))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
