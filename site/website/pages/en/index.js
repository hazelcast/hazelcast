const React = require('react');

const CompLibrary = require('../../core/CompLibrary.js');

const MarkdownBlock = CompLibrary.MarkdownBlock; /* Used to read markdown */
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
        {props.title}
        <small>{props.tagline}</small>
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
        <div className="inner">
        <Logo img_src={`${baseUrl}img/logo-icon-dark.svg`} />
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
        background={props.background}>
        <GridBlock
          align="center"
          contents={props.children}
          layout={props.layout}
        />
      </Container>
    );

    const Features = () => (
      <Block layout="twoColumn">
        {[
          {
            title: 'Create a Cluster within Seconds',
            image: `${baseUrl}img/undraw_relaxation_blue.svg`,
            imageAlign: 'top',
            content: 'It\'s easy to get started with Jet. The nodes automatically discover each' +
                     ' other to form a cluster, both in a cloud environment and on your laptop.' +
                     ' This is great for quick testing and simplifies deployment and maintenance.',
          },
          {
            title: 'Build Fault-Tolerant Data Pipelines that Scale',
            image: `${baseUrl}img/undraw_working_out_orange.svg`,
            imageAlign: 'top',
            content: 'Process your data using a rich library of transforms such as windowing,' +
                     ' joins and aggregations. Jet keeps processing data without loss even' +
                     ' when a node fails, and as soon as you add another node, it starts' +
                     ' sharing the computation load. First-class support for Apache Kafka,' +
                     ' Hadoop, Hazelcast and many other data sources and sinks.'
          },
          {
            title: 'Massively Parallel with Predictable Latency',
            image: `${baseUrl}img/undraw_finish_line_blue.svg`,
            imageAlign: 'top',
            content: 'Jet\'s core execution engine was designed for high throughput with low' +
            ' system overhead and latency. It uses a fixed-size thread pool to run any number of' +
            ' parallel tasks. The engine is based on coroutines that implement suspendable' +
            ' computation, allowing many of them to run concurrently on a single thread.'
          },
          {
            title: 'Single Java Binary',
            image: `${baseUrl}img/undraw_superhero_orange.svg`,
            imageAlign: 'top',
            content: 'Jet is a single 12 MB Java archive (JAR). It’s lightweight enough to run' +
                     ' on small devices, you can embed it into your application as just another' +
                     ' dependency or deploy it as a standalone cluster. First-class support for' +
                     ' Kubernetes is included.'
          },
        ]}
      </Block>
    );

    return (
      <div>
        <HomeSplash siteConfig={siteConfig} language={language} />
        <div className="mainContainer">
          <Features />
        </div>
      </div>
    );
  }
}

module.exports = Index;
