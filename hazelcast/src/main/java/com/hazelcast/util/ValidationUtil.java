/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util;

import static java.lang.String.format;

/**
 * A utility class for validating arguments and state.
 */
public class ValidationUtil {

    public static String hasText(String argument, String argName){
        isNotNull(argument,argName);

        if(argument.isEmpty()){
            throw new IllegalArgumentException(format("argument '%s' can't be an empty string",argName));
        }

        return argument;
    }

    public static <E> E isNotNull(E argument, String argName){
        if(argument == null){
           throw new IllegalArgumentException(format("argument '%s' can't be null",argName));
        }

        return argument;
    }

    private ValidationUtil(){}
}
