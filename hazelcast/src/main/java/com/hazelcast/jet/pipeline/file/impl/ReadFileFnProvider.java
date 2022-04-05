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

package com.hazelcast.jet.pipeline.file.impl;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.pipeline.file.FileFormat;

import javax.annotation.Nonnull;
import java.nio.file.Path;
import java.util.stream.Stream;

/**
 * Provides a mapping function from a {@code Path} to a {@code Stream} of
 * items emitted from a local filesystem source. It is specialized to a
 * single {@link FileFormat}, whose ID you can retrieve from the {@link
 * #format} method.
 */
public interface ReadFileFnProvider {

    /**
     * Takes a {@link FileFormat} and uses it to create and return a function
     * that maps a {@code Path} on the local filesystem to a stream of items
     * that the file source should emit.
     */
    @Nonnull
    <T> FunctionEx<Path, Stream<T>> createReadFileFn(@Nonnull FileFormat<T> format);

    /**
     * Returns a string that identifies the {@link FileFormat} supported by
     * this function provider.
     */
    @Nonnull
    String format();
}
