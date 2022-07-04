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

package com.hazelcast.jet.cdc;

import com.hazelcast.jet.annotation.EvolvingApi;

import javax.annotation.Nonnull;
import java.util.Map;

/**
 * Represents the top-level component of a {@link ChangeRecord}, such as
 * the <em>key</em> or the <em>value</em>. Since these components are JSON
 * expressions, this is actually a generic wrapper around a JSON expression.
 * Contains various methods for retrieving component values or for mapping
 * itself to data objects.
 *
 * @since Jet 4.2
 */
@EvolvingApi
public interface RecordPart {

    /**
     * Maps the entire element to an instance of the specified class.
     * <p>
     * Parsing is based on <a href="https://github.com/FasterXML/jackson-jr">
     * Jackson jr</a> with <a
     * href="https://github.com/FasterXML/jackson-jr/tree/master/jr-annotation-support">
     * annotation support</a>, so the supplied class can be annotated accordingly.
     * <p>
     * Note: there is a neat trick for converting types during object mapping.
     * Let's say we have a {@code birth_date} column in a table of type
     * {@code DATE} and we want to map it to a field named {@code birthDate}
     * in our row object, of type {@code LocalDate}. We would write
     * code like this:
     * <pre>
     *  {@code
     *  public LocalDate birthDate;
     *
     *  public void setBirthDate(long dateInEpochDays) {
     *      this.birthDate = dateInEpochDays == 0 ? null : LocalDate.ofEpochDay(dateInEpochDays);
     *  }
     *  }
     * </pre>
     * The things to note here is that by specifying a setter and giving
     * it the input parameter type of {@code long} we force the object
     * mapping to interpret the column value as a {@code long} (as opposed
     * to {@code Date}). Then we can take care of {@code null} handling
     * and converting to whatever desired type ourselves.
     *
     * @return object of type {@code T}, obtained as the result of the mapping
     * @throws ParsingException if the mapping fails to produce a result
     */
    @Nonnull
    <T> T toObject(@Nonnull Class<T> clazz) throws ParsingException;

    /**
     * Presents a parsed form of the underlying JSON message as a {@code Map}.
     * The keys are the top-level fields from the JSON and the values can range
     * from simple strings, numbers, collections and sub-maps.
     * <p>
     * Parsing is based on <a href="https://github.com/FasterXML/jackson-jr">
     * Jackson jr</a>, you can refer to its documentation for more details.
     *
     * @return {@code Map} representation of the JSON data
     * @throws ParsingException if the underlying JSON message fails to
     * parse
     */
    @Nonnull
    Map<String, Object> toMap() throws ParsingException;

    /**
     * Returns the raw JSON string that this object wraps. You can use it if
     * parsing is failing for some reason (for example on some untested
     * DB-connector version combination).
     */
    @Nonnull
    String toJson();

}
