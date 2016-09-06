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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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

public class AssemblyPlannerPlatformTest extends PlatformTestCase
  {
  public AssemblyPlannerPlatformTest()
    {
    super( false );
    }

  @Test
  public void testSimpleAssembly() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileApache );

    AssemblyPlanner planner = new AssemblyPlanner()
    {
    @Override
    public Map<String, String> getFlowDescriptor()
      {
      return Collections.emptyMap();
      }

    @Override
    public List<Pipe> resolveTails( Context context )
      {
      Pipe pipe = new Pipe( (String) context.getFlow().getSourceNames().get( 0 ) );

      pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

      pipe = new GroupBy( pipe, new Fields( "ip" ) );

      pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );

      return Arrays.asList( pipe );
      }
    };

    Tap sink = getPlatform().getTextFile( getOutputPath( "simple" ), SinkMode.REPLACE );

    FlowDef flowDef = FlowDef.flowDef()
      .addSource( "test", source )
      .addSink( "test", sink )
      .addAssemblyPlanner( planner );

    Flow flow = getPlatform().getFlowConnector().connect( flowDef );

    flow.complete();

    validateLength( flow, 8 );
    }

  @Test
  public void testCompositeAssembly() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileApache );

    Pipe pipe = new Pipe( "test" );

    AssemblyPlanner lazyEach = new AssemblyPlanner()
    {
    @Override
    public List<Pipe> resolveTails( Context context )
      {
      Pipe pipe = new Each( context.getTails().get( 0 ), new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

      return Arrays.asList( pipe );
      }

    @Override
    public Map<String, String> getFlowDescriptor()
      {
      return Collections.EMPTY_MAP;
      }
    };

    AssemblyPlanner lazyCount = new AssemblyPlanner()
    {
    @Override
    public List<Pipe> resolveTails( Context context )
      {
      Pipe pipe = new GroupBy( context.getTails().get( 0 ), new Fields( "ip" ) );

      pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );

      return Arrays.asList( pipe );
      }

    @Override
    public Map<String, String> getFlowDescriptor()
      {
      return Collections.EMPTY_MAP;
      }

    };

    Tap sink = getPlatform().getTextFile( getOutputPath( "composite" ), SinkMode.REPLACE );

    FlowDef flowDef = FlowDef.flowDef()
      .addSource( "test", source )
      .addSink( "test", sink )
      .addTail( pipe )
      .addAssemblyPlanner( lazyEach )
      .addAssemblyPlanner( lazyCount );

    Flow flow = getPlatform().getFlowConnector().connect( flowDef );

    flow.complete();

    validateLength( flow, 8 );
    }
  }
