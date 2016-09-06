/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.operation.debug;

import java.util.Collection;
import java.util.Map;

import cascading.PlatformTestCase;
import cascading.TestConstants;
import cascading.flow.Flow;
import cascading.flow.FlowConnectorProps;
import cascading.flow.planner.BaseFlowStep;
import cascading.operation.AssertionLevel;
import cascading.operation.Debug;
import cascading.operation.DebugLevel;
import cascading.operation.Operation;
import cascading.operation.assertion.AssertMatches;
import cascading.operation.assertion.AssertNotNull;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexParser;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import org.junit.Test;

/**
 *
 */
public class BuildDebugPlatformTest extends PlatformTestCase
  {
  public BuildDebugPlatformTest()
    {
    }

  /**
   * verify lone group assertion fails
   *
   * @throws Exception
   */
  @Test
  public void testDebugLevels() throws Exception
    {
    Tap source = getPlatform().getTextFile( "input" );
    Tap sink = getPlatform().getTextFile( "output" );

    Pipe pipe = new Pipe( "test" );

    String regex = TestConstants.APACHE_COMMON_REGEX;
    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip", "time", "method", "event", "status", "size" ), regex, new int[]{
      1, 2, 3, 4, 5, 6} ) );

    pipe = new Each( pipe, AssertionLevel.STRICT, new AssertNotNull() );

    pipe = new Each( pipe, DebugLevel.DEFAULT, new Debug() );

    pipe = new Each( pipe, DebugLevel.VERBOSE, new Debug() );

    pipe = new Each( pipe, new Fields( "method" ), new RegexFilter( "^POST" ) );

    pipe = new Each( pipe, new Fields( "method" ), AssertionLevel.STRICT, new AssertMatches( "^POST" ) );

    pipe = new GroupBy( pipe, new Fields( "method" ) );

    Map<Object, Object> properties = getProperties();

    // test default config case
    assertEquals( getDebugCount( getPlatform().getFlowConnector( properties ).connect( source, sink, pipe ) ), 1 );

    FlowConnectorProps.setDebugLevel( properties, DebugLevel.DEFAULT );
    assertEquals( getDebugCount( getPlatform().getFlowConnector( properties ).connect( source, sink, pipe ) ), 1 );

    FlowConnectorProps.setDebugLevel( properties, DebugLevel.VERBOSE );
    assertEquals( getDebugCount( getPlatform().getFlowConnector( properties ).connect( source, sink, pipe ) ), 2 );

    FlowConnectorProps.setDebugLevel( properties, DebugLevel.NONE );
    assertEquals( getDebugCount( getPlatform().getFlowConnector( properties ).connect( source, sink, pipe ) ), 0 );
    }

  private int getDebugCount( Flow flow )
    {
    BaseFlowStep step = (BaseFlowStep) flow.getFlowSteps().get( 0 );

    Collection<Operation> operations = step.getAllOperations();
    int count = 0;

    for( Operation operation : operations )
      {
      if( operation instanceof Debug )
        count++;
      }

    return count;
    }
  }