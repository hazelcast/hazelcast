package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.impl.client.SqlPage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static com.hazelcast.client.impl.protocol.ClientMessage.BEGIN_FRAME;
import static com.hazelcast.client.impl.protocol.ClientMessage.END_FRAME;
import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.fastForwardToEndFrame;

public class SqlPageCodec {


    private SqlPageCodec() {
    }

    public static void encode(ClientMessage clientMessage, SqlPage sqlPage) {
        clientMessage.add(BEGIN_FRAME.copy());

        byte[] content = new byte[]{(byte) (sqlPage.isLast() ? 1 : 0)};
        clientMessage.add(new ClientMessage.Frame(content));

        List<SqlColumnType> columnTypes = sqlPage.getColumnTypes();
        ListIntegerCodec.encode(clientMessage, columnTypes.stream().map(SqlColumnType::getId).collect(Collectors.toList()));

        Iterator<List<Object>> iterator = sqlPage.getColumns().iterator();
        for (SqlColumnType sqlColumnType : sqlPage.getColumnTypes()) {
            Collection<Object> collection = iterator.next();
            switch (sqlColumnType) {
                case INTEGER:
                    ListIntegerCodec.encode(clientMessage, (Collection<Integer>) (Object) collection);
                    break;
                case BIGINT:
                    ListLongCodec.encode(clientMessage, (Collection<Long>) (Object) collection);
                    break;
                case OBJECT:
                    ListMultiFrameCodec.encode(clientMessage, (Collection<Data>) (Object) collection, DataCodec::encodeNullable);
                    break;
                case VARCHAR:
                    ListMultiFrameCodec.encodeContainsNullable(clientMessage,
                        (Collection<String>) (Object) collection, StringCodec::encode);
                    break;
            }
        }

        clientMessage.add(END_FRAME.copy());
    }

    public static SqlPage decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        boolean isLast = iterator.next().content[0] == 1;

        ArrayList<SqlColumnType> types = ListIntegerCodec.decode(iterator).stream()
            .map(SqlColumnType::getById).collect(Collectors.toCollection(ArrayList::new));

        //handle empty iterator
        List<List<Object>> result = new ArrayList<>(types.size());
        for (SqlColumnType type : types) {
            switch (type) {
                case INTEGER:
                    result.add((List<Object>) (Object) ListIntegerCodec.decode(iterator));
                    break;
                case BIGINT:
                    result.add((List<Object>) (Object) ListLongCodec.decode(iterator));
                    break;
                case OBJECT:
                    result.add(ListMultiFrameCodec.decode(iterator, DataCodec::decodeNullable));
                    break;
                case VARCHAR:
                    result.add(ListMultiFrameCodec.decodeContainsNullable(iterator, StringCodec::decode));
                    break;
                default:
                    throw new IllegalStateException("Unknown type " + type);
            }
        }

        fastForwardToEndFrame(iterator);
        return new SqlPage(types, result, isLast);
    }
}
