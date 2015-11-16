/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.extractor;

/**
 * Wrapper for the arguments passed to the {@link ValueExtractor#extract(Object, Arguments, ValueCollector)} method.
 * <p/>
 * ValueExtractor may use custom arguments if they are specified by the user in the query.
 * The custom arguments may be passed within the square brackets located after the name of the attribute
 * that uses a ValueExtractor, like: customAttribute[argumentString]
 * <p/>
 * Let's have a look at the following query: 'currency[incoming] == EUR'
 * Let's assume that currency is an custom attribute that uses com.test.CurrencyExtractor for extraction.
 * The string "incoming" is an argument that will be passed to the {@link ArgumentsParser} during the extraction.
 * The parser will parse the string according to the parser's custom logic and it will return an Arguments object.
 * The Arguments object may return a single object, array, collection, etc. It's up to the ValueExtractor implementor's
 * to understand the semantics of the arguments.
 * <p/>
 * By default, if there's no custom {@link ArgumentsParser} registered, the default parser follows the pass-through
 * semantics, which means that the string that's in the square-brackets is passes "as-is" to the Arguments class.
 * <p/>
 * <i>For now, it's not possible to register custom ArgumentsParsers. It will be possible in future releases.</i>
 *
 * @param <T> type of the argument object encompassed by this class
 */
public abstract class Arguments<T> {

    /**
     * @return the argument object encompassed by this class
     */
    public abstract T get();

}
