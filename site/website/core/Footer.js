const React = require('react');

class Footer extends React.Component {
  docUrl(doc, language) {
    const baseUrl = this.props.config.baseUrl;
    const docsUrl = this.props.config.docsUrl;
    const docsPart = `${docsUrl ? `${docsUrl}/` : ''}`;
    const langPart = `${language ? `${language}/` : ''}`;
    return `${baseUrl}${docsPart}${langPart}${doc}`;
  }

  pageUrl(doc, language) {
    const baseUrl = this.props.config.baseUrl;
    return baseUrl + (language ? `${language}/` : '') + doc;
  }

  render() {
    const Blog = require('docusaurus/lib/core/MetadataBlog.js');
    const {getPath} = require('docusaurus/lib/core/utils.js');
    return (
      <footer className="nav-footer" id="footer">
        <section className="sitemap">
          <div style={{textAlign: "left"}}> 
          <a href={this.props.config.baseUrl} className="nav-home">
            {this.props.config.footerIcon && (
              <img
                src={this.props.config.baseUrl + this.props.config.footerIcon}
                alt={this.props.config.title}
                width="200"
                height="40"
              />
            )}
          </a>
          <div style={{marginLeft: "12px"}}>
          <a
              className="github-button"
              href={this.props.config.repoUrl}
              data-icon="octicon-star"
              data-count-href="/facebook/docusaurus/stargazers"
              data-show-count="true"
              data-count-aria-label="# stargazers on GitHub"
              aria-label="Star this project on GitHub">
              Star On GitHub
          </a>
          </div>
          </div>
          <div>
            <h5>Docs</h5>
            <a href={this.docUrl('get-started/intro')}>Get Started</a>
            <a href={this.docUrl('concepts/dag')}>Concepts</a>
            <a href={this.docUrl('tutorials/kafka')}>Tutorials</a>
            <a href={this.docUrl('architecture/distributed-computing')}>Architecture</a>
            <a href={this.docUrl('operations/installation')}>Operations Guide</a>
            <a href={this.docUrl('enterprise')}>Enterprise Edition</a>
          </div>
          <div>
            <h5>Community</h5>
            <a
              href="https://groups.google.com/forum/#!forum/hazelcast-jet"
              target="_blank"
              rel="noreferrer noopener">
              Google Groups
            </a>
            <
              a
              href="http://stackoverflow.com/questions/tagged/hazelcast-jet"
              target="_blank"
              rel="noreferrer noopener">
              Stack Overflow
            </a>
            <a href="https://slack.hazelcast.com">Slack</a>
          </div>
          <div>
            <h5>Latest From the Blog</h5>
            {
              Blog.slice(0,5).map( blog => {
              return <a key={blog.id} href={`${this.props.config.baseUrl}blog/${getPath(blog.path,this.props.config.cleanUrl)}`}>{blog.title}</a>
              })
            }
          </div>
          <div>
            <h5>More</h5>
            {/* <a href={`${this.props.config.baseUrl}blog`}>Blog</a> */}
            <a href="https://github.com/hazelcast/hazelcast-jet">GitHub Project</a>
            <a href="http://hazelcast.com/company/careers/">Work at Hazelcast</a>
            <a href={this.pageUrl('license')}>License</a>
          </div>
        </section>
        <section className="copyright">{this.props.config.copyright}</section>
      </footer>
    );
  }
}

module.exports = Footer;
