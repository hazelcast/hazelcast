package com.hazelcast.internal.metrics.renderers;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;


@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class CommaSeparatedKeyValueProbeRendererTest {
    private CommaSeparatedKeyValueProbeRenderer renderer;

    @Before
    public void setup() {
        renderer = new CommaSeparatedKeyValueProbeRenderer();
    }

    @Test
    public void test() {
        renderer.start();
        renderer.renderLong("long", 1);
        renderer.renderDouble("double", 2);
        renderer.renderException("exception", new RuntimeException("somemessage"));
        renderer.renderNoValue("novalue");
        renderer.finish();

        String s = renderer.getResult();

        assertTrue(s.startsWith("{time="));
        assertTrue(s.endsWith(",long=1,double=2.0,exception=NA,novalue=NA}\n"));
    }
}
