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

import java.net.URL;

import cascading.PlatformTestCase;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import org.junit.Test;

import static data.InputData.*;

public class ClasspathPlatformTest extends PlatformTestCase
  {
  public ClasspathPlatformTest()
    {
    super( true );
    }

  public static class TestFunction extends BaseOperation implements Function
    {
    @Override
    public void operate( FlowProcess flowProcess, FunctionCall functionCall )
      {
      URL resource = Thread.currentThread().getContextClassLoader().getResource( testClasspathJarContents );

      if( resource == null )
        throw new RuntimeException( "cannot find resource" );

      functionCall.getOutputCollector().add( functionCall.getArguments() );
      }
    }

  @Test
  public void testSimpleClasspath() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileApache );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new TestFunction(), Fields.RESULTS );

    Tap sink = getPlatform().getTextFile( getOutputPath( "classpath" ), SinkMode.REPLACE );

    FlowDef flowDef = FlowDef.flowDef()
      .addSource( "test", source )
      .addTailSink( pipe, sink )
      .addToClassPath( testClasspathJar );

    Flow flow = getPlatform().getFlowConnector().connect( flowDef );

    flow.complete();

    validateLength( flow, 10 );
    }
  }
