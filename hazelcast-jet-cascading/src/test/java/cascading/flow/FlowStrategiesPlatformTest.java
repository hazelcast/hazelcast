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

package cascading.flow;

import java.util.List;

import cascading.PlatformTestCase;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexParser;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import org.junit.Test;

import static data.InputData.inputFileApache;

public class FlowStrategiesPlatformTest extends PlatformTestCase
  {
  public FlowStrategiesPlatformTest()
    {
    super( false ); // leave cluster testing disabled
    }

  @Test
  public void testSkipStrategiesReplace() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( inputFileApache );

    // !!! enable replace
    Tap sink = getPlatform().getTextFile( getOutputPath( "replace" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    sink.deleteResource( flow.getConfig() );

    assertTrue( "default skip", !flow.getFlowSkipStrategy().skipFlow( flow ) );
    assertTrue( "exist skip", !new FlowSkipIfSinkExists().skipFlow( flow ) );

    flow.complete();

    assertTrue( "default skip", !flow.getFlowSkipStrategy().skipFlow( flow ) );
    assertTrue( "exist skip", !new FlowSkipIfSinkExists().skipFlow( flow ) );

    FlowSkipStrategy old = flow.getFlowSkipStrategy();

    FlowSkipStrategy replaced = flow.setFlowSkipStrategy( new FlowSkipIfSinkExists() );

    assertTrue( "not same instance", old == replaced );

    validateLength( flow.openSource(), 10 ); // validate source, this once, as a sanity check
    validateLength( flow, 10, null );
    }

  @Test
  public void testSkipStrategiesKeep() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( inputFileApache );

    // !!! enable replace
    Tap sink = getPlatform().getTextFile( getOutputPath( "keep" ), SinkMode.KEEP );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    sink.deleteResource( flow.getConfig() );

    assertTrue( "default skip", !flow.getFlowSkipStrategy().skipFlow( flow ) );
    assertTrue( "exist skip", !new FlowSkipIfSinkExists().skipFlow( flow ) );

    flow.complete();

    assertTrue( "default skip", flow.getFlowSkipStrategy().skipFlow( flow ) );
    assertTrue( "exist skip", new FlowSkipIfSinkExists().skipFlow( flow ) );

    validateLength( flow.openSource(), 10 ); // validate source, this once, as a sanity check
    validateLength( flow, 10, null );
    }

  @Test
  public void testFlowStepStrategy() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileApache );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    pipe = new GroupBy( pipe, new Fields( "ip" ) );

    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );

    Tap sink = getPlatform().getTextFile( getOutputPath( "simple" ), SinkMode.REPLACE );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    final boolean[] wasApplied = {false};
    flow.setFlowStepStrategy( new FlowStepStrategy()
    {
    @Override
    public void apply( Flow flow, List predecessorSteps, FlowStep flowStep )
      {
      wasApplied[ 0 ] = true;
      assertTrue( predecessorSteps.isEmpty() );
      }
    } );

    flow.complete();

    assertTrue( wasApplied[ 0 ] );
    validateLength( flow, 8, null );
    }

  }