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

package com.hazelcast.jet.pipeline.file;

import javax.annotation.Nonnull;

/**
 * {@code FileFormat} for binary files where the whole file is one {@code
 * byte[]} item. See {@link FileFormat#bytes} for more details.
 *
 * @since Jet 4.4
 */
public class RawBytesFileFormat implements FileFormat<byte[]> {

    /**
     * Format ID for raw binary data.
     */
    public static final String FORMAT_BIN = "bin";

    private static final long serialVersionUID = 1L;

    /**
     * Create {@link RawBytesFileFormat}. See {@link FileFormat#bytes()} for more
     * details.
     */
    RawBytesFileFormat() {
    }

    @Nonnull
    @Override
    public String format() {
        return FORMAT_BIN;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof RawBytesFileFormat;
    }

    @Override
    public int hashCode() {
        return 0;
    }
}
