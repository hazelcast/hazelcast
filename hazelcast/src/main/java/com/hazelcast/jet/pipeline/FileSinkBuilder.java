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

package com.hazelcast.jet.pipeline;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.Processor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static com.hazelcast.jet.core.processor.SinkProcessors.writeFileP;

/**
 * See {@link Sinks#filesBuilder}.
 *
 * @param <T> type of the items the sink accepts
 * @since Jet 3.0
 */
public final class FileSinkBuilder<T> {

    /**
     * A suffix added to file names until they are committed. Files
     * ending with this suffix should be ignored when processing. See
     * {@link Sinks#filesBuilder} for more information.
     */
    public static final String TEMP_FILE_SUFFIX = ".tmp";

    /**
     * A value to pass to {@link #rollByFileSize(long)} if you want to
     * disable rolling by file size.
     */
    public static final long DISABLE_ROLLING = Long.MAX_VALUE;

    private final String directoryName;

    private FunctionEx<? super T, String> toStringFn = Object::toString;
    private Charset charset = StandardCharsets.UTF_8;
    private String datePattern;
    private long maxFileSize = DISABLE_ROLLING;
    private boolean exactlyOnce = true;

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
     * <p>
     * The given function must be stateless and {@linkplain
     * Processor#isCooperative() cooperative}.
     */
    @Nonnull
    public FileSinkBuilder<T> toStringFn(@Nonnull FunctionEx<? super T, String> toStringFn) {
        this.toStringFn = toStringFn;
        return this;
    }

    /**
     * Sets the character set used to encode the files. Default value is {@link
     * java.nio.charset.StandardCharsets#UTF_8}.
     */
    @Nonnull
    public FileSinkBuilder<T> charset(@Nonnull Charset charset) {
        this.charset = charset;
        return this;
    }

    /**
     * Sets a date pattern that will be included in the file name. Each time
     * the formatted current time changes a new file will be started. For
     * example, if the {@code datePattern} is {@code yyyy-MM-dd}, a new file
     * will be started every day.
     * <p>
     * The rolling is based on system time, not on event time. By default no
     * rolling by date is done. If the system clock goes back, the outcome is
     * unspecified and possibly corrupt.
     *
     * @since Jet 4.0
     */
    @Nonnull
    public FileSinkBuilder<T> rollByDate(@Nullable String datePattern) {
        this.datePattern = datePattern;
        return this;
    }

    /**
     * Enables rolling by file size. If the size after writing a batch of items
     * exceeds the limit, a new file will be started. From this follows that
     * the file will typically be larger than the given maximum.
     * <p>
     * To disable rolling after certain size, pass {@code
     * DISABLE_ROLLING}. This is the default value.
     *
     * @since Jet 4.0
     */
    @Nonnull
    public FileSinkBuilder<T> rollByFileSize(long maxFileSize) {
        Preconditions.checkPositive(1, "rolling size must be a positive number");
        this.maxFileSize = maxFileSize;
        return this;
    }

    /**
     * Enables or disables the exactly-once behavior of the sink using
     * two-phase commit of state snapshots. If enabled, the {@linkplain
     * JobConfig#setProcessingGuarantee(ProcessingGuarantee) processing
     * guarantee} of the job must be set to {@linkplain
     * ProcessingGuarantee#EXACTLY_ONCE exactly-once}, otherwise the sink's
     * guarantee will match that of the job. In other words, sink's
     * guarantee cannot be higher than job's, but can be lower to avoid the
     * additional overhead.
     * <p>
     * See {@link Sinks#filesBuilder(String)} for more information.
     * <p>
     * The default value is true.
     *
     * @param enable If true, sink's guarantee will match the job guarantee.
     *               If false, sink's guarantee will be at-least-once even if
     *               job's is exactly-once
     * @return this instance for fluent API
     * @since Jet 4.0
     */
    @Nonnull
    public FileSinkBuilder<T> exactlyOnce(boolean enable) {
        exactlyOnce = enable;
        return this;
    }

    /**
     * Creates and returns the file {@link Sink} with the supplied components.
     */
    @Nonnull
    public Sink<T> build() {
        return Sinks.fromProcessor("filesSink(" + directoryName + ')',
                writeFileP(directoryName, charset, datePattern, maxFileSize, exactlyOnce, toStringFn));
    }
}
