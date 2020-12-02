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

package com.hazelcast.sql.impl.calcite.opt.metadata;

import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdRowCount;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.util.BuiltInMethod;

/**
 * Metadata which provides row count estimates.
 */
public final class HazelcastRelMdRowCount extends RelMdRowCount {
    /** Do not change the name (see {@code RelMetadataQueryBase} JavaDoc). */
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(BuiltInMethod.ROW_COUNT.method, new HazelcastRelMdRowCount());

    private HazelcastRelMdRowCount() {
        // No-op.
    }

    // Empty at the moment, will be extended in the future.
    // Now it serves as an example of how to wire up a metadata handler into Calcite.
}
