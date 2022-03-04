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

package com.hazelcast.internal.config.mergepolicies;

import com.hazelcast.spi.merge.MergingCosts;
import com.hazelcast.spi.merge.MergingExpirationTime;

/**
 * Custom merge policy which requires a complex set of composed merge
 * types ({@link MergingExpirationTime} and {@link MergingCosts}).
 * <p>
 * It also tries to crash the config validation with multiple other generics,
 * like the parameterized {@link Comparable} or a simpler {@link Number}.
 * <p>
 * It also uses inheritance to ensure that we check the complete class hierarchy.
 */
@SuppressWarnings("unused")
public class ComplexCustomMergePolicy<N extends Number, C extends Comparable<N>, U>
        extends ComplexCustomBaseMergePolicy {
}
