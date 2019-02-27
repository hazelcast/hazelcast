/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.function.FunctionEx;

import javax.annotation.Nonnull;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static com.hazelcast.jet.core.processor.SinkProcessors.writeFileP;

/**
 * See {@link Sinks#filesBuilder}.
 *
 * @param <T> type of the items the sink accepts
 */
public final class FileSinkBuilder<T> {

    private final String directoryName;

    private FunctionEx<? super T, String> toStringFn = Object::toString;
    private Charset charset = StandardCharsets.UTF_8;
    private boolean append;

    /**
     * Use {@link Sinks#filesBuilder}.
     */
    FileSinkBuilder(@Nonnull String directoryName) {
        this.directoryName = directoryName;
    }

    /**
     * Sets the function which converts the item to its string representation.
     * Each item is followed with a platform-specific line separator. Default
     * value is {@link Object#toString}.
     */
    public FileSinkBuilder<T> toStringFn(@Nonnull FunctionEx<? super T, String> toStringFn) {
        this.toStringFn = toStringFn;
        return this;
    }

    /**
     * Sets the character set used to encode the files. Default value is {@link
     * java.nio.charset.StandardCharsets#UTF_8}.
     */
    public FileSinkBuilder<T> charset(@Nonnull Charset charset) {
        this.charset = charset;
        return this;
    }

    /**
     * Sets whether to append ({@code true}) or overwrite ({@code false})
     * an existing file. Default value is {@code false}.
     */
    public FileSinkBuilder<T> append(boolean append) {
        this.append = append;
        return this;
    }

    /**
     * Creates and returns the file {@link Sink} with the supplied components.
     */
    public Sink<T> build() {
        return Sinks.fromProcessor("filesSink(" + directoryName + ')',
                writeFileP(directoryName, toStringFn, charset, append));
    }
}
