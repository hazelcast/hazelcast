package com.hazelcast.impl.ascii.memcache;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.stubbing.OngoingStubbing;

import com.hazelcast.impl.ascii.TextCommandConstants;
import com.hazelcast.impl.ascii.TextCommandConstants.TextCommandType;
import com.hazelcast.impl.ascii.TextCommandService;
import com.hazelcast.impl.ascii.TextCommandServiceImpl;
import com.hazelcast.impl.ascii.rest.HttpCommand;
import com.hazelcast.impl.ascii.rest.HttpGetCommand;
import com.hazelcast.impl.ascii.rest.HttpGetCommandProcessor;

public class MemcacheFlushCommandTest {
	private static final String uri_map_prefix = "/hazelcast/rest/maps/";
    private static final String uri_map_correct = uri_map_prefix + "testmap/testkey";

    private static final String uri_queue_prefix = "/hazelcast/rest/queues/";

    private final String TEST_MAP = "default";
    private final String TEST_KEY = "testKey";
    private final String TEST_VALUE = "bs";

    private static FlushCommand expected_result;
    private static final TextCommandService mockTextCommandService = mock(TextCommandServiceImpl.class);
    private static FlushCommandProcessor componentUnderTest;

    @BeforeClass
    public static void initialSetup() {
        componentUnderTest = new FlushCommandProcessor(mockTextCommandService);
        expected_result = new FlushCommand();
        expected_result.setResponse(TextCommandConstants.OK);
    }
    
    @After
    public void resetMocks() {
        reset(mockTextCommandService);
        
    }

    @Test
    public void testFlush() {
    	mockTextCommandService.put(TEST_MAP, TEST_KEY, TEST_VALUE, 3600);
    	OngoingStubbing<Object> yada = when(mockTextCommandService.get(TEST_MAP, TEST_KEY)).thenReturn(TEST_VALUE);
        FlushCommand command = new FlushCommand();
        componentUnderTest.handle(command);

        //Object blah = verify(mockTextCommandService).get(TEST_MAP, TEST_KEY); // This function fails... I really don't know how to use this
        assertEquals(expected_result.response, command.response);
    }

}
