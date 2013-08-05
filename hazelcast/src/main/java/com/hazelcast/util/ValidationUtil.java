package com.hazelcast.util;

import static java.lang.String.format;

/**
 * A utility class for validating arguments and state.
 */
public class ValidationUtil {

    public static <E> E argNotNull(E argument, String argName){
        if(argument == null){
           throw new IllegalArgumentException(format("argument '%s' can't be null",argName));
        }

        return argument;
    }

    private ValidationUtil(){}
}
