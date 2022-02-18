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

package com.hazelcast.query.impl;

import com.hazelcast.query.extractor.ValueExtractor;

/**
 * Common superclass for all argument parsers.
 * <p>
 * For now, it's not possible to register a custom {@link ArgumentParser}, thus a default parser is used.
 * It follows a "pass-through" semantics, which means that the string located in the square-brackets is passed
 * "as-is" to the extract method.
 * <p>
 * <b>It is not allowed to use square brackets within the argument string.</b>
 *
 * @param <I> type of the unparsed argument object (input)
 * @param <O> type of the parsed argument object (output)
 */
@FunctionalInterface
public interface ArgumentParser<I, O> {

    /**
     * This method takes the unparsed argument object as an input and returns
     * the parsed output that will be used by the {@link ValueExtractor}.
     *
     * @param input extraction argument in the specified format
     * @return parsed argument object that will be passed to the {@link ValueExtractor}
     */
    O parse(I input);

}
