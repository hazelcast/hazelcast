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
