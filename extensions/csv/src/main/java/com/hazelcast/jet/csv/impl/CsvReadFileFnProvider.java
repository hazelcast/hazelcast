/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.csv.impl;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser.Feature;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.pipeline.file.CsvFileFormat;
import com.hazelcast.jet.pipeline.file.FileFormat;
import com.hazelcast.jet.pipeline.file.impl.ReadFileFnProvider;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import java.io.FileInputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.hazelcast.jet.impl.util.Util.createFieldProjection;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.util.Spliterator.ORDERED;
import static java.util.function.Function.identity;

/**
 * {@link ReadFileFnProvider} for CSV files, reading the given path and
 * deserializing using Jackson {@link CsvMapper}.
 */
@SuppressFBWarnings(
        value = "OBL_UNSATISFIED_OBLIGATION",
        justification = "The FileInputStream is closed via Stream$onClose"
)
public class CsvReadFileFnProvider implements ReadFileFnProvider {

    @SuppressWarnings("unchecked")
    @Nonnull
    @Override
    public <T> FunctionEx<Path, Stream<T>> createReadFileFn(@Nonnull FileFormat<T> format) {
        CsvFileFormat<T> csvFileFormat = (CsvFileFormat<T>) format;
        Class<?> formatClazz = csvFileFormat.clazz(); // Format is not Serializable

        return path -> {
            FileInputStream fis = new FileInputStream(path.toFile());

            MappingIterator<T> iterator;
            Function<T, T> projection = identity();
            if (formatClazz == String[].class) {
                ObjectReader reader = new CsvMapper().enable(Feature.WRAP_AS_ARRAY)
                                                     .readerFor(String[].class)
                                                     .with(CsvSchema.emptySchema().withSkipFirstDataRow(false));

                iterator = reader.readValues(fis);
                if (!iterator.hasNext()) {
                    throw new JetException("Header row missing in " + path);
                }
                String[] header = (String[]) iterator.next();
                List<String> fieldNames = csvFileFormat.fieldNames();
                if (fieldNames != null) {
                    projection = (Function<T, T>) createFieldProjection(header, fieldNames);
                }
            } else {
                iterator = new CsvMapper().readerFor(formatClazz)
                                          .withoutFeatures(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                                          .with(CsvSchema.emptySchema().withHeader())
                                          .readValues(fis);
            }
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, ORDERED), false)
                    .map(projection)
                    .onClose(() -> uncheckRun(fis::close));
        };
    }

    @Nonnull
    @Override
    public String format() {
        return CsvFileFormat.FORMAT_CSV;
    }
}
