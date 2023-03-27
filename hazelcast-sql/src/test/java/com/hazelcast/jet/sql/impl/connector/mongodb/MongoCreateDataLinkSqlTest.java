package com.hazelcast.jet.sql.impl.connector.mongodb;

import com.hazelcast.datalink.DataLink;
import com.hazelcast.jet.mongodb.datalink.MongoDataLink;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MongoCreateDataLinkSqlTest extends MongoSqlTest {

    @Test
    public void test() {
        String dlName = randomName();
        instance().getSql().execute("CREATE DATA LINK " + dlName
                + " TYPE \"" + MongoDataLink.class.getName() + "\" "
                + options());

        DataLink dataLink = getNodeEngineImpl(
                instance()).getDataLinkService().getAndRetainDataLink(dlName, MongoDataLink.class);

        assertThat(dataLink).isNotNull();
        assertThat(dataLink.getName()).isEqualTo(dlName);
        assertThat(dataLink.getConfig().getClassName()).isEqualTo(MongoDataLink.class.getName());
    }
}
