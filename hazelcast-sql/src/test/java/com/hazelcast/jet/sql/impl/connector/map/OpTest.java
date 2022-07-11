package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.JetGetJobAndSqlSummaryListCodec;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.jet.impl.JobAndSqlSummary;
import com.hazelcast.jet.json.JsonUtil;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

@Category({QuickTest.class, ParallelJVMTest.class})
public class OpTest extends SqlTestSupport {
    @BeforeClass
    public static void setUpClass() {
        initializeWithClient(1, null, null);
    }

    @Test
    public void doIt() throws ExecutionException, InterruptedException {
        SqlResult sqlResult = instance().getSql().execute("select * from table(generate_stream(1))");
        HazelcastClientProxy client = (HazelcastClientProxy) client();
        ClientInvocation inv = new ClientInvocation(client.client, JetGetJobAndSqlSummaryListCodec.encodeRequest(),
                null, client.client.getClientClusterService().getMasterMember().getUuid());
        ClientMessage response = inv.invoke().get();
        String decodedResponse = JetGetJobAndSqlSummaryListCodec.decodeResponse(response);
        try {
            List<JobAndSqlSummary> jobAndSqlSummaries = JsonUtil.listFrom(decodedResponse, JobAndSqlSummary.class);
            jobAndSqlSummaries.forEach(System.err::println);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.err.println("Jest --> " + decodedResponse);
    }
}
