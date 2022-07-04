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
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.impl.util.IOUtil;
import com.hazelcast.jet.json.JsonUtil;
import com.hazelcast.jet.pipeline.file.FileFormat;
import com.hazelcast.jet.pipeline.file.JsonFileFormat;
import com.hazelcast.jet.pipeline.file.LinesTextFileFormat;
import com.hazelcast.jet.pipeline.file.ParquetFileFormat;
import com.hazelcast.jet.pipeline.file.RawBytesFileFormat;
import com.hazelcast.jet.pipeline.file.TextFileFormat;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Stream;

import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.util.Objects.requireNonNull;

/**
 * Implementation of FileSourceFactory for the local filesystem.
 */
public class LocalFileSourceFactory implements FileSourceFactory {

    private static Map<String, ReadFileFnProvider> readFileFnProviders;

    static {
        Map<String, ReadFileFnProvider> mapFns = new HashMap<>();

        addMapFnProvider(mapFns, new JsonReadFileFnProvider());
        addMapFnProvider(mapFns, new LinesReadFileFnProvider());
        addMapFnProvider(mapFns, new ParquetReadFileFnProvider());
        addMapFnProvider(mapFns, new RawBytesReadFileFnProvider());
        addMapFnProvider(mapFns, new TextReadFileFnProvider());

        ServiceLoader<ReadFileFnProvider> loader = ServiceLoader.load(ReadFileFnProvider.class);
        for (ReadFileFnProvider readFileFnProvider : loader) {
            addMapFnProvider(mapFns, readFileFnProvider);
        }

        LocalFileSourceFactory.readFileFnProviders = Collections.unmodifiableMap(mapFns);
    }

    private static void addMapFnProvider(Map<String, ReadFileFnProvider> mapFns, ReadFileFnProvider provider) {
        mapFns.put(provider.format(), provider);
    }

    @Nonnull @Override
    public <T> ProcessorMetaSupplier create(@Nonnull FileSourceConfiguration<T> fsc) {
        FileFormat<T> format = requireNonNull(fsc.getFormat());
        ReadFileFnProvider readFileFnProvider = readFileFnProviders.get(format.format());
        if (readFileFnProvider == null) {
            throw new JetException("Could not find ReadFileFnProvider for FileFormat: " + format.format() + ". " +
                    "Did you provide correct modules on classpath?");
        }
        FunctionEx<Path, Stream<T>> mapFn = readFileFnProvider.createReadFileFn(format);
        return SourceProcessors.readFilesP(fsc.getPath(), fsc.getGlob(), fsc.isSharedFileSystem(),
                fsc.isIgnoreFileNotFound(), mapFn);
    }

    @SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION")
    private abstract static class AbstractReadFileFnProvider implements ReadFileFnProvider {

        @Nonnull @Override
        public <T> FunctionEx<Path, Stream<T>> createReadFileFn(@Nonnull FileFormat<T> format) {
            FunctionEx<InputStream, Stream<T>> mapInputStreamFn = mapInputStreamFn(format);
            return path -> {
                FileInputStream fis = new FileInputStream(path.toFile());
                return mapInputStreamFn.apply(fis).onClose(() -> uncheckRun(fis::close));
            };
        }

        @Nonnull
        abstract <T> FunctionEx<InputStream, Stream<T>> mapInputStreamFn(FileFormat<T> format);
    }

    private static class JsonReadFileFnProvider implements ReadFileFnProvider {

        @Nonnull
        @Override
        @SuppressWarnings("unchecked")
        public <T> FunctionEx<Path, Stream<T>> createReadFileFn(@Nonnull FileFormat<T> format) {
            JsonFileFormat<T> jsonFileFormat = (JsonFileFormat<T>) format;
            Class<T> formatClazz = jsonFileFormat.clazz();

            return path -> {
                // Jackson doesn't handle empty files
                if (path.toFile().length() == 0) {
                    return Stream.empty();
                }

                if (formatClazz == null) {
                    return (Stream<T>) JsonUtil.mapSequenceFrom(path);
                } else {
                    return JsonUtil.beanSequenceFrom(path, formatClazz);
                }
            };
        }

        @Nonnull
        @Override
        public String format() {
            return JsonFileFormat.FORMAT_JSON;
        }
    }

    private static class LinesReadFileFnProvider extends AbstractReadFileFnProvider {

        @Nonnull @Override
        @SuppressWarnings("unchecked")
        <T> FunctionEx<InputStream, Stream<T>> mapInputStreamFn(FileFormat<T> format) {
            LinesTextFileFormat linesTextFileFormat = (LinesTextFileFormat) format;
            String thisCharset = linesTextFileFormat.charset().name();
            return is -> {
                BufferedReader reader = new BufferedReader(new InputStreamReader(is, thisCharset));
                return (Stream<T>) reader.lines().onClose(() -> uncheckRun(reader::close));
            };
        }

        @Nonnull @Override
        public String format() {
            return LinesTextFileFormat.FORMAT_LINES;
        }
    }

    private static class ParquetReadFileFnProvider implements ReadFileFnProvider {

        @Nonnull @Override
        public <T> FunctionEx<Path, Stream<T>> createReadFileFn(@Nonnull FileFormat<T> format) {
            throw new UnsupportedOperationException("Reading Parquet files is not supported in local filesystem mode." +
                    " " +
                    "Use Jet Hadoop module with FileSourceBuilder.useHadoopForLocalFiles option instead.");
        }

        @Nonnull @Override
        public String format() {
            return ParquetFileFormat.FORMAT_PARQUET;
        }
    }

    private static class RawBytesReadFileFnProvider extends AbstractReadFileFnProvider {

        @Nonnull @Override
        @SuppressWarnings("unchecked")
        <T> FunctionEx<InputStream, Stream<T>> mapInputStreamFn(FileFormat<T> format) {
            return is -> (Stream<T>) Stream.of(IOUtil.readFully(is));
        }

        @Nonnull @Override
        public String format() {
            return RawBytesFileFormat.FORMAT_BIN;
        }
    }

    private static class TextReadFileFnProvider extends AbstractReadFileFnProvider {

        @Nonnull @Override
        @SuppressWarnings("unchecked")
        <T> FunctionEx<InputStream, Stream<T>> mapInputStreamFn(FileFormat<T> format) {
            TextFileFormat textFileFormat = (TextFileFormat) format;
            String thisCharset = textFileFormat.charset().name();
            return is -> (Stream<T>) Stream.of(new String(IOUtil.readFully(is), Charset.forName(thisCharset)));
        }

        @Nonnull @Override
        public String format() {
            return TextFileFormat.FORMAT_TXT;
        }
    }
}
