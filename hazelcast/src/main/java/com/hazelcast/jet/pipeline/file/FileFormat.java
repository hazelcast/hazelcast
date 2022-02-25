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
import javax.annotation.Nullable;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.List;

/**
 * Describes the data format of a file to be used as a Jet data source.
 * This is a data object that holds the configuration; actual
 * implementation code is looked up elsewhere, by using this object as a
 * key.
 *
 * @param <T> the type of items a source using this file format will emit
 * @since Jet 4.4
 */
public interface FileFormat<T> extends Serializable {

    /**
     * Returns the name of the file format. The convention is to use the
     * well-known filename suffix or, if there is none, a short-form name of
     * the format.
     */
    @Nonnull
    String format();

    // Factory methods for supported file formats are here for easy discoverability.

    /**
     * Returns a file format for Avro files.
     */
    @Nonnull
    static <T> AvroFileFormat<T> avro() {
        return avro(null);
    }

    /**
     * Returns a file format for Avro files that specifies to use reflection
     * to deserialize the data into instances of the provided Java class.
     * Jet will use the {@code ReflectDatumReader} to read Avro data. The
     * parameter may be {@code null}, disabling the option to deserialize
     * using reflection, but for that case you may prefer the no-argument
     * {@link #avro()} call.
     */
    @Nonnull
    static <T> AvroFileFormat<T> avro(@Nullable Class<T> clazz) {
        return new AvroFileFormat<T>().withReflect(clazz);
    }

    /**
     * Returns a file format for CSV files which specifies to deserialize each
     * line into {@code String[]}. It assumes the CSV has a header line and
     * specifies to use it as the column names that map to the object's fields.
     * <p>
     * {@code fieldNames} specify which column should be at which index in the
     * resulting string array. It is useful if the files have different field
     * order or don't have the same set of columns.
     * <p>
     * For example, if the argument is {@code [surname, name]}, then the format
     * will always return items of type String[2] where at index 0 is the
     * {@code surname} column and at index 1 is the {@code name} column,
     * regardless of the actual columns found in a particular file. If some
     * file doesn't have some field, the value at its index will always be 0.
     * <p>
     * If the given list is {@code null}, the length and order of the string
     * array will match the order found in each file. It can be different for
     * each file. If it's an empty array, a zero-length array will be returned.
     */
    @Nonnull
    static CsvFileFormat<String[]> csv(@Nullable List<String> fieldNames) {
        return new CsvFileFormat<>(fieldNames);
    }

    /**
     * Returns a file format for CSV files which specifies to deserialize each
     * line into an instance of the given class. It assumes the CSV has a
     * header line and specifies to use it as the column names that map to the
     * object's fields.
     */
    @Nonnull
    static <T> CsvFileFormat<T> csv(@Nonnull Class<T> clazz) {
        return new CsvFileFormat<>(clazz);
    }

    /**
     * Returns a file format for JSON Lines files.
     */
    @Nonnull
    static <T> JsonFileFormat<T> json() {
        return json(null);
    }

    /**
     * Returns a file format for JSON Lines files, where each line of text
     * is one JSON object. It specifies to deserialize the JSON data into
     * instances of the provided class. It uses <a
     * href="https://github.com/FasterXML/jackson-jr">Jackson jr</a>, which
     * supports the basic data types such as strings, numbers, lists and maps,
     * objects with JavaBeans-style getters/setters, as well as public fields.
     * If parameter is {@code null}, data is deserialized into {@code
     * Map<String, Object>} but for that case you may prefer the no-argument
     * {@link #json()} call.
     */
    @Nonnull
    static <T> JsonFileFormat<T> json(@Nullable Class<T> clazz) {
        return new JsonFileFormat<T>().withClass(clazz);
    }

    /**
     * Returns a file format for text files where each line is a {@code String}
     * data item. It uses the UTF-8 character encoding.
     */
    @Nonnull
    static LinesTextFileFormat lines() {
        return new LinesTextFileFormat();
    }

    /**
     * Returns a file format for text files where each line is a {@code String}
     * data item. This variant allows you to choose the character encoding.
     * Note that the Hadoop-based file connector only accepts UTF-8.
     *
     * @param charset character encoding of the file
     */
    @Nonnull
    static LinesTextFileFormat lines(@Nonnull Charset charset) {
        return new LinesTextFileFormat(charset);
    }

    /**
     * Returns a file format for Parquet files.
     * <p>
     * <strong>NOTE:</strong> this format is supported only through the Hadoop
     * connector.
     */
    @Nonnull
    static <T> ParquetFileFormat<T> parquet() {
        return new ParquetFileFormat<>();
    }

    /**
     * Returns a file format for binary files.
     */
    @Nonnull
    static RawBytesFileFormat bytes() {
        return new RawBytesFileFormat();
    }

    /**
     * Returns a file format for text files where the whole file is a single
     * string item. It uses the UTF-8 character encoding.
     */
    @Nonnull
    static TextFileFormat text() {
        return new TextFileFormat();
    }

    /**
     * Returns a file format for text files where the whole file is a single
     * string item. This variant allows you to choose the character encoding.
     * <p>
     * <strong>NOTE:</strong> the Hadoop connector only supports UTF-8. This
     * option is supported for local files only.
     *
     * @param charset character encoding of the file
     */
    @Nonnull
    static TextFileFormat text(@Nonnull Charset charset) {
        return new TextFileFormat(charset);
    }
}
