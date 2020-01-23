package com.hazelcast.internal.logstore;

import com.hazelcast.log.CloseableIterator;
import com.hazelcast.log.encoders.StringEncoder;
import org.junit.Test;

import java.util.Iterator;

public class LocalIteratorTest extends AbstractLogStoreTest {

    @Test
    public void test(){
        LogStore<String> logStore = createObjectLogStore(new LogStoreConfig()
                .setSegmentSize(1024)
                .setEncoder(new StringEncoder()));

        for(int k=0;k<1000;k++){
            logStore.putObject("foo-"+k);
        }

        CloseableIterator<String> iterator = logStore.iterator();
        while (iterator.hasNext()){
            System.out.println(iterator.next());
        }
        iterator.close();
    }
}
