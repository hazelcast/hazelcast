package com.hazelcast.impl.ascii.memcache;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.stubbing.OngoingStubbing;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.impl.FactoryImpl;
import com.hazelcast.impl.FactoryImpl.HazelcastInstanceProxy;
import com.hazelcast.impl.ascii.TextCommandConstants;
import com.hazelcast.impl.ascii.TextCommandConstants.TextCommandType;
import com.hazelcast.impl.ascii.TextCommandService;
import com.hazelcast.impl.ascii.TextCommandServiceImpl;
import com.hazelcast.impl.ascii.rest.HttpCommand;
import com.hazelcast.impl.ascii.rest.HttpGetCommand;
import com.hazelcast.impl.ascii.rest.HttpGetCommandProcessor;
import com.hazelcast.impl.base.FactoryAwareNamedProxy;

public class FlushCommandTest {
    private final String TEST_MAP = "default";
    private final String TEST_KEY = "testKey";
    private final String TEST_VALUE = "test_value";

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
    
    @AfterClass
    public static void init() throws Exception {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testFlushCmd() {
    	mockTextCommandService.put(TEST_MAP, TEST_KEY, TEST_VALUE, 3600);
    	when(mockTextCommandService.get(TEST_MAP, TEST_KEY)).thenReturn(TEST_VALUE);
        FlushCommand command = new FlushCommand();
        componentUnderTest.handle(command);

        // Check if flush called
        assertEquals(expected_result.response, command.response);
    }
    
    @Test
    public void testFlushMap()
    {
    	// Create instance
        HazelcastInstance defaultInst = Hazelcast.newHazelcastInstance(new Config());
        
        // Create new map
        IMap<Object, Object> testMap = defaultInst.getMap(TEST_MAP);
        testMap.put(TEST_KEY, TEST_VALUE);
        
        // Make sure value has been stored
    	assertEquals(testMap.size(), 1);
    	
    	// Flush and test if empty
    	TextCommandServiceImpl txtCmd = new TextCommandServiceImpl(((HazelcastInstanceProxy)defaultInst).getFactory().node);
    	txtCmd.flush(TEST_MAP);
    	assertEquals(testMap.size(), 0);
    }

}
