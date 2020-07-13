const React = require('react');

const CompLibrary = require('../../core/CompLibrary.js');
const Fragment = React.Fragment;
const Container = CompLibrary.Container;
const GridBlock = CompLibrary.GridBlock;

class HomeSplash extends React.Component {
  render() {
    const {siteConfig, language = ''} = this.props;
    const {baseUrl, docsUrl} = siteConfig;
    const docsPart = `${docsUrl ? `${docsUrl}/` : ''}`;
    const langPart = `${language ? `${language}/` : ''}`;
    const docUrl = doc => `${baseUrl}${docsPart}${langPart}${doc}`;

    const SplashContainer = props => (
      <div className="homeContainer">
        <div className="homeSplashFade">
          <div className="wrapper homeWrapper">{props.children}</div>
        </div>
      </div>
    );

    const Logo = props => (
      <div className="projectLogo">
        <img src={props.img_src} alt="Project Logo" />
      </div>
    );

    const ProjectTitle = props => (
      <h2 className="projectTitle">
        Use Your Data Instantly At Scale
        <small>Hazelcast Jet is a distributed computing platform that serves and processes massive amounts of
           live data with consistent low latency.
        </small>
      </h2>
    );

    const PromoSection = props => (
      <div className="section promoSection">
        <div className="promoRow">
          <div className="pluginRowBlock">{props.children}</div>
        </div>
      </div>
    );

    const Button = props => (
      <div className="pluginWrapper buttonWrapper">
        <a className="button" href={props.href} target={props.target}>
          {props.children}
        </a>
      </div>
    );

    return (

      <SplashContainer>
        {/* <Logo img_src={`${baseUrl}img/logo-icon-dark.svg`} /> */}
        <div className="inner">
        <ProjectTitle tagline={siteConfig.tagline} title={siteConfig.title} />
        <PromoSection>
          <Button href={docUrl('get-started/intro')}>Get Started</Button>
          <Button href="https://github.com/hazelcast/hazelcast-jet">View on GitHub</Button>
        </PromoSection>
        </div>
      </SplashContainer>
    );
  }
}

class Index extends React.Component {
  render() {
    const {config: siteConfig, language = ''} = this.props;
    const {baseUrl} = siteConfig;

    const Block = props => (
      <Container
        id={props.id}
        background={props.background}
        >
        <GridBlock
          align={props.align}
          className="features"
          contents={props.children}
          layout={props.layout}
        />
      </Container>
    );

    const Features = () => (
      <Block layout="fourColumn"  align='center'>
        {[
           {
            title: 'Build Data Pipelines That Scale',
            image: `${baseUrl}img/share.svg`,
            imageAlign: 'top',
            content: 'Jet consumes and analyzes millions of events per second'
            + ' or terabytes of data at rest using a <a href="/docs/api/pipeline">unified API</a>. '
            + 'Jet keeps processing data <a href="/docs/architecture/fault-tolerance">without loss</a> even '
            + ' when a node fails. You can add nodes to a live cluster, and they immediately start'
            + ' sharing the computation load.'

          },
          {
            title: 'Granular and Fast Access to Data',
            image: `${baseUrl}img/database.svg`,
            imageAlign: 'top',
            content: 'Jet stores computational state in <a href="/docs/api/data-structures">fault-tolerant, '
             + 'distributed in-memory storage</a>, allowing thousands of concurrent users granular '
             + 'and fast access to your data without breaking a sweat.'
          },
          {
            title: 'Sub 10ms Latency at the 99.99th Percentile',
            image: `${baseUrl}img/timer.svg`,
            imageAlign: 'top',
            content: 'Jet\'s core execution engine was designed for high throughput and low'
            + ' overhead and latency. In rigorous tests, it stayed within a'
            + ' <a href="blog/2020/06/23/jdk-gc-benchmarks-rematch">10-millisecond 99.99% latency ceiling</a>'
            + ' for windowed stream aggregation. The engine uses '
            + ' <a href="/docs/architecture/execution-engine">coroutines</a>'
            + ' that implement suspendable computation, allowing it to run hundreds of'
            + ' concurrent jobs on a fixed number of threads.'
          },
          {
            title: 'Production-Ready Out of the Box',
            image: `${baseUrl}img/construction-and-tools.svg`,
            imageAlign: 'top',
            content: 'Jet nodes <a href="/docs/operations/discovery">automatically discover</a> each'
                     + ' other to form a cluster, both in a'
                     + ' <a href="/docs/operations/kubernetes">cloud environment</a> and on your laptop.'
                     + ' It is lightweight enough to'
                     + ' run on a Raspberry Pi. No ZooKeeper or Hadoop cluster required for production.'
          },

        ]}
      </Block>

    );

    const UseCases = () => (
      <div className="useCases">
        <Block id="data-layer" background="dark">
        {[
          {
            title: 'Serve Operational Data for Applications, Fast',
            image: `${baseUrl}img/layers2.svg`,
            imageAlign: 'left',
            content: ' Ingest data from a <a href="/docs/tutorials/cdc">relational database</a>,'
            + ' <a href="docs/api/sources-sinks#hadoop-inputformatoutputformat">Hadoop, Amazon S3, Google Cloud Storage, Azure Data Lake</a> and more,'
            + ' combine it with real-time sources '
            + ' and serve through thousands of concurrent low-latency queries and fine-grained, key-based access.'
          },
        ]}
      </Block>
      <Block id="real-time" background="dark">
      {[
        {
          title: 'React Instantly To Real-Time Data At Scale',
          image: `${baseUrl}img/electricity.svg`,
          imageAlign: 'right',
          content: 'You can instantly react to real-time events with Jet, querying, enriching and applying inference at scale. A single node is capable of <a href="/docs/tutorials/windowing">windowing and aggregating</a> '
          + '100Hz sensor data from 100,000 devices with '
          + '  <a href="blog/2020/06/23/jdk-gc-benchmarks-rematch">latencies below 10 milliseconds</a>: that\'s 10 million events/second. Jet works'
          + ' with many streaming data sources such as <a href="docs/tutorials/kafka">Apache Kafka</a>, Apache Pulsar, or message brokers such as RabbitMQ.'
        },
      ]}
      </Block>
      <Block id="real-time" background="dark">
      {[
        {
          title: 'Build Stateful Workflows',
          image: `${baseUrl}img/process.svg`,
          imageAlign: 'left',
          content: 'Use Jet to build distributed and stateful workflows. Ingest data, denormalize and process it, '
          + ' run a series of distributed computations and cache the intermediate results in <a href="/docs/api/data-structures">queryable memory</a>'
          + ' and finally write the results '
          + ' to your <a href="/docs/api/sources-sinks">destination of choice</a>.'
        }
      ]}
      </Block>
    </div>
    );

    const Users = () => {
      return <div className="productShowcaseSection">
        <h1>Who is Using Hazelcast Jet?</h1>
        <div className="logos">
          <a href="https://www.adobe.com"><img src={`${baseUrl}img/logos/adobe.svg`}></img></a>
          <a href="https://www.anz.com.au"><img src={`${baseUrl}img/logos/anz.svg`}></img></a>
          <a href="https://www.betgenius.com/"><img src={`${baseUrl}img/logos/betgenius.svg`}></img></a>
          <a href="https://www.bnpparibas.pl/"><img src={`${baseUrl}img/logos/bnp-paribas.svg`}></img></a>
          <a href="https://www.cgi.com"><img src={`${baseUrl}img/logos/cgi.svg`}></img></a>
          <a href="https://www.codedose.com"><img src={`${baseUrl}img/logos/codedose.png`}></img></a>
          <a href="https://www.finantix.com"><img src={`${baseUrl}img/logos/finantix.png`}></img></a>
          <a href="https://www.flowtraders.com"><img src={`${baseUrl}img/logos/flowtraders.png`}></img></a>
          <a href="https://www.plex.com/"><img src={`${baseUrl}img/logos/plex.svg`}></img></a>
          <a href="https://www.sigmastream.com/"><img src={`${baseUrl}img/logos/sigmastream.png`}></img></a>
        </div>
    </div>

    };
    return (
      <div>
        <HomeSplash siteConfig={siteConfig} language={language} />
        <div className="mainContainer">
          <Features />
          <div style={{textAlign: 'center'}}>
            <h1>Use Hazelcast Jet To</h1>
          </div>
          <UseCases />
          <Users />
        </div>
      </div>
    );
  }
}

module.exports = Index;
