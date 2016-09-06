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

package cascading.pipe;

import java.util.HashMap;
import java.util.Map;

import cascading.PlatformTestCase;
import cascading.flow.Flow;
import cascading.operation.Function;
import cascading.operation.regex.RegexSplitter;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import org.junit.Test;

/**
 *
 */
public class MergeGroupPlatformTest extends PlatformTestCase
  {
  public MergeGroupPlatformTest()
    {
    }

  @Test
  public void testBuildMerge()
    {
    Tap sourceLower = getPlatform().getTextFile( "file1" );
    Tap sourceUpper = getPlatform().getTextFile( "file2" );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Tap sink = getPlatform().getTextFile( "outpath", SinkMode.REPLACE );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Pipe splice = new GroupBy( "merge", Pipe.pipes( pipeLower, pipeUpper ), new Fields( "num" ), null, false );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice );
    }

  @Test
  public void testBuildMergeFail()
    {
    Tap sourceLower = getPlatform().getTextFile( "file1" );
    Tap sourceUpper = getPlatform().getTextFile( "file2" );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Function splitter1 = new RegexSplitter( new Fields( "num", "foo" ), " " );
    Function splitter2 = new RegexSplitter( new Fields( "num", "bar" ), " " );

    Tap sink = getPlatform().getTextFile( "outpath", SinkMode.REPLACE );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter1 );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter2 );

    Pipe splice = new GroupBy( "merge", Pipe.pipes( pipeLower, pipeUpper ), new Fields( "num" ), null, false );

    try
      {
      Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice );
      fail( "did not fail on mismatched field names" );
      }
    catch( Exception exception )
      {
      // test passes
      }
    }
  }
