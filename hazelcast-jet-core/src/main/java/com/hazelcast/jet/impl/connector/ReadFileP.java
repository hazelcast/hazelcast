/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorSupplier;

import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;

/**
 * @see com.hazelcast.jet.Processors#readFile(String, Charset, String)
 */
public class ReadFileP extends AbstractProcessor {

    private final Charset charset;
    private final int parallelism;
    private final int id;
    private final Path directory;
    private final String glob;

    ReadFileP(String directory, Charset charset, String glob, int parallelism, int id) {
        this.directory = Paths.get(directory);
        this.glob = glob;
        this.charset = charset;
        this.parallelism = parallelism;
        this.id = id;
    }

    @Override
    public boolean complete() {
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(directory, glob == null ? "*" : glob)) {
            StreamSupport.stream(directoryStream.spliterator(), false)
                    .filter(this::shouldProcessEvent)
                    .forEach(this::processFile);
        } catch (IOException e) {
            throw sneakyThrow(e);
        }

        return true;
    }

    private boolean shouldProcessEvent(Path file) {
        int hashCode = file.hashCode();
        return ((hashCode & Integer.MAX_VALUE) % parallelism) == id;
    }

    private void processFile(Path file) {
        if (getLogger().isFinestEnabled()) {
            getLogger().finest("Processing file " + file);
        }

        try (BufferedReader reader = Files.newBufferedReader(file, charset)) {
            for (String line; (line = reader.readLine()) != null; ) {
                emit(line);
            }
        } catch (IOException e) {
            throw sneakyThrow(e);
        }
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    /**
     * @see com.hazelcast.jet.Processors#readFile(String, Charset, String)
     */
    public static ProcessorSupplier supplier(String directory, String charset, String glob) {
        return new ProcessorSupplier() {
            static final long serialVersionUID = 1L;

            @Nonnull
            @Override
            public Collection<? extends Processor> get(int count) {
                Charset charsetObj = charset == null ? StandardCharsets.UTF_8 : Charset.forName(charset);
                return IntStream.range(0, count)
                        .mapToObj(i -> new ReadFileP(directory, charsetObj, glob, count, i))
                        .collect(Collectors.toList());
            }
        };
    }
}
