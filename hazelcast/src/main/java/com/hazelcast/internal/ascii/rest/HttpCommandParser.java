package com.hazelcast.internal.ascii.rest;

import com.hazelcast.internal.ascii.CommandParser;
import com.hazelcast.internal.ascii.TextCommand;
import com.hazelcast.internal.ascii.memcache.ErrorCommand;
import com.hazelcast.internal.nio.ascii.TextDecoder;
import com.hazelcast.internal.server.ServerConnection;

import java.util.StringTokenizer;

import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.ERROR_CLIENT;

abstract class HttpCommandParser<HC extends HttpCommand> implements CommandParser {

    @Override
    public final TextCommand parser(TextDecoder decoder, String cmd, int space) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public final TextCommand parser(TextDecoder decoder, String cmd, int space, ServerConnection connection) {
        StringTokenizer st = new StringTokenizer(cmd);
        st.nextToken();
        String uri;
        if (st.hasMoreTokens()) {
            uri = st.nextToken();
        } else {
            return new ErrorCommand(ERROR_CLIENT);
        }
        return createHttpCommand(decoder, uri, connection);
    }

    abstract HC createHttpCommand(TextDecoder decoder, String uri, ServerConnection connection);
}
