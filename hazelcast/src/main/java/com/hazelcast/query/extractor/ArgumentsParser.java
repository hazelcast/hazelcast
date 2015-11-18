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
 * Common superclass for all argument parsers.
 *
 * @param <T> type of the argument object encompassed in the {@link Arguments} result.
 */
public abstract class ArgumentsParser<T> {

    /**
     * ValueExtractor may use custom arguments if they are specified by the user in the query.
     * The custom arguments may be passed within the square brackets located after the name of the attribute
     * that uses a ValueExtractor, like: customAttribute[argumentString]
     * <p/>
     * This method takes the 'argumentString' object as an input and returns the Arguments object that encompasses
     * parsed arguments as an output.
     *
     * @param input extraction arguments in the specified format
     * @return Arguments object that encompasses parsed arguments
     */
    public abstract Arguments<T> parse(Object input);

}
