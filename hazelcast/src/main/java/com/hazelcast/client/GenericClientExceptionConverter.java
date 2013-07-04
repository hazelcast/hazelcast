package com.hazelcast.client;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * @author mdogan 7/3/13
 */
final class GenericClientExceptionConverter implements ClientExceptionConverter {

    public Object convert(Throwable t) {
        StringWriter s = new StringWriter();
        t.printStackTrace(new PrintWriter(s));
        return new GenericError(s.toString(), 0);
    }
}
