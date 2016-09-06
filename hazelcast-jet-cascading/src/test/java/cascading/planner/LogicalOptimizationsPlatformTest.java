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

package cascading.planner;

import java.util.Map;

import cascading.PlatformTestCase;
import cascading.cascade.Cascades;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexParser;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import org.junit.Ignore;
import org.junit.Test;

import static data.InputData.inputFileApache;


@Ignore // TODO: enable when we have support for custom rules
public class LogicalOptimizationsPlatformTest extends PlatformTestCase
  {
  public LogicalOptimizationsPlatformTest()
    {
    super( false ); // not necessary
    }

  /**
   * If the sinks have the same scheme as a temp tap, replace the temp tap
   *
   * @throws Exception
   */
  @Test
  public void testChainedTaps() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileApache );

    Pipe pipe = new Each( new Pipe( "first" ), new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );
    pipe = new GroupBy( pipe, new Fields( "ip" ) );

    pipe = new Each( new Pipe( "second", pipe ), new Fields( "ip" ), new RegexFilter( "7" ) );
    pipe = new GroupBy( pipe, new Fields( "ip" ) );

    pipe = new Each( new Pipe( "third", pipe ), new Fields( "ip" ), new RegexFilter( "6" ) );
    pipe = new GroupBy( pipe, new Fields( "ip" ) );

    Tap sinkFirst = getPlatform().getTabDelimitedFile( new Fields( "ip" ), getOutputPath( "chainedtaps/first" ), SinkMode.REPLACE );
    Tap sinkSecond = getPlatform().getTabDelimitedFile( new Fields( "ip" ), getOutputPath( "chainedtaps/second" ), SinkMode.REPLACE );
    Tap sinkThird = getPlatform().getTabDelimitedFile( new Fields( "ip" ), getOutputPath( "chainedtaps/third" ), SinkMode.REPLACE );

    Map<String, Tap> sinks = Cascades.tapsMap( new String[]{"first", "second",
                                                            "third"}, Tap.taps( sinkFirst, sinkSecond, sinkThird ) );
    FlowConnector flowConnector = getPlatform().getFlowConnector();

    Flow flow = flowConnector.connect( source, sinks, pipe );

    if( getPlatform().isMapReduce() )
      assertEquals( "wrong number of steps", 3, flow.getFlowSteps().size() );

    flow.complete();

    validateLength( flow, 3 );
    }
  }
