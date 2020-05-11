/*
 * Copyright 2020 Hazelcast Inc.
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

package com.hazelcast.jet.cdc;

import com.hazelcast.jet.annotation.EvolvingApi;

import javax.annotation.Nonnull;
import java.util.Map;

/**
 * High level component of a {@link ChangeRecord}, such as the <i>key</i>
 * or the <i>value</i> of it, but can also be thought of as a simple
 * convenience wrapper of a JSON expression. Contains various methods
 * for retrieving component values or for mapping itself to data
 * objects.
 *
 * @since 4.2
 */
@EvolvingApi
public interface RecordPart {

    /**
     * Maps the entire element to an instance of the specified class.
     * <p>
     * Parsing it is based on <a
     * href="https://github.com/FasterXML/jackson-jr">Jackson jr</a>,
     * with <a
     * href="https://github.com/FasterXML/jackson-jr/tree/master/jr-annotation-support">annotation
     * support</a>, so the parameter class can be annotated accordingly.
     *
     * @return object of type {@code T}, obtained as the result of the
     * mapping
     * @throws ParsingException if the mapping fails to produce a result
     */
    @Nonnull
    <T> T toObject(@Nonnull Class<T> clazz) throws ParsingException;

    /**
     * Presents a parsed form of the underlying JSON message, as a
     * {@code Map}. The keys in the map are the top level fields from
     * the JSON and the values can range from simple strings, numbers,
     * collections and sub-maps.
     * <p>
     * Parsing it is based on <a
     * href="https://github.com/FasterXML/jackson-jr">Jackson jr</a>,
     * that's where further details can be found.
     *
     * @return {@code Map} representation of the JSON data
     * @throws ParsingException if the underlying JSON message fails to
     * parse
     */
    @Nonnull
    Map<String, Object> toMap() throws ParsingException;

    /**
     * Returns raw JSON string which the content of this event element
     * is based on. To be used when parsing fails for some reason (for
     * example on some untested DB-connector version combination).
     */
    @Nonnull
    String toJson();

}
