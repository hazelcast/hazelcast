package com.hazelcast.jet.sql.impl.validate.operators.udf;

public class MyFun extends ScalarUserDefinedFunctionDefinition {
    public static String eval(String arg) {
        return arg + "/" + arg;
    }
}
