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

package com.hazelcast.cardinality.impl.hyperloglog.impl;

enum HyperLogLogEncoding {

    /**
     * Higher precision and lower memory footprint encoding relying on LINEARCOUNTING
     * for estimates, suitable for small range cardinalities, or until its growing footprint
     * meets the DENSE constant size.
     */
    SPARSE,

    /**
     * Lower precision and constant memory footprint encoding relying on original HyperLogLog
     * computations for estimates, and bias corrections from HyperLogLog++. Suitable for mid to large
     * range cardinalities.
     */
    DENSE
}
