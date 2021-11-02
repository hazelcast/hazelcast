package com.hazelcast.internal.ascii.rest;

import static com.hazelcast.internal.util.StringUtil.stringToBytes;

public enum HttpStatusCode {

    SC_100(100, stringToBytes("HTTP/1.1 100 Continue\r\n")),
    SC_200(200, stringToBytes("HTTP/1.1 200 OK\r\n")),
    SC_204(204, stringToBytes("HTTP/1.1 204 No Content\r\n")),
    SC_400(400, stringToBytes("HTTP/1.1 400 Bad Request\r\n")),
    SC_403(403, stringToBytes("HTTP/1.1 403 Forbidden\r\n")),
    SC_404(404, stringToBytes("HTTP/1.1 404 Not Found\r\n")),
    SC_500(500, stringToBytes("HTTP/1.1 500 Internal Server Error\r\n")),
    SC_503(503, stringToBytes("HTTP/1.1 503 Service Unavailable\r\n"))
    ;

    final int code;
    final byte[] statusLine;

    HttpStatusCode(int code, byte[] statusLine) {
        this.code = code;
        this.statusLine = statusLine;
    }
}
