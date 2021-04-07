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

import javax.annotation.Nonnull;
import java.nio.charset.Charset;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * {@link FileFormat} for text files where the whole file is one {@code
 * String} data item. See {@link FileFormat#text} for more details.
 *
 * @since 4.4
 */
public class TextFileFormat implements FileFormat<String> {

    /**
     * Format ID for text files.
     */
    public static final String FORMAT_TXT = "txt";

    private final String charset;

    /**
     * Creates a {@code TextFileFormat} with the default character encoding
     * (UTF-8).
     */
    TextFileFormat() {
        this(UTF_8);
    }

    /**
     * Creates a {@code TextFileFormat} with the provided character
     * encoding (UTF-8).
     * <p>
     * <strong>NOTE:</strong> the Hadoop connector only supports UTF-8. This
     * option is supported for local files only.
     */
    TextFileFormat(@Nonnull Charset charset) {
        this.charset = requireNonNull(charset, "charset must not be null").name();
    }

    /**
     * Returns the configured character encoding.
     */
    @Nonnull
    public Charset charset() {
        return Charset.forName(charset);
    }

    @Nonnull @Override
    public String format() {
        return FORMAT_TXT;
    }
}
