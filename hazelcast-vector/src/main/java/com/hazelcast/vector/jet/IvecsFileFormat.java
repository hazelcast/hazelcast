/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.jet;

import com.hazelcast.internal.serialization.BinaryInterface;
import com.hazelcast.jet.pipeline.file.FileFormat;
import com.hazelcast.spi.annotation.Beta;

import javax.annotation.Nonnull;
import java.io.Serial;
import java.util.Map;

/**
 * ivecs format definition for Unified File Connector
 *
 * @since 5.5
 */
@Beta
@BinaryInterface
public class IvecsFileFormat implements FileFormat<Map.Entry<Integer, int[]>> {
    /**
     * Format id for ivecs.
     */
    public static final String IVECS_FILE_FORMAT = "ivecs";

    @Serial
    private static final long serialVersionUID = 1L;

    @Nonnull
    @Override
    public String format() {
        return IVECS_FILE_FORMAT;
    }
}
