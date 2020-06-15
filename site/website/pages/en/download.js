/**
 * Copyright (c) 2017-present, Facebook, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

const React = require('react');
const CompLibrary = require('../../core/CompLibrary');
const Container = CompLibrary.Container;

const CWD = process.cwd();

const versions = require(`${CWD}/all-versions.json`);
const MarkdownBlock = CompLibrary.MarkdownBlock;

function Downloads(props) {
  const {config: siteConfig} = props;
  const latest = versions[0];
  const repoUrl = `https://github.com/${siteConfig.organizationName}/${siteConfig.projectName}`;
  return (
    <div className="docMainWrapper wrapper">
      <Container className="mainContainer versionsContainer">
        <div>
          <header>
            <h1>{siteConfig.title} Downloads</h1>
          </header>
          <MarkdownBlock>
            The Hazelcast Jet download package includes Hazelcast Jet server and 
            several additional modules. It requires a JDK to run, which can be obtained from 
            [AdoptOpenJDK](https://adoptopenjdk.net) (minimum version is 8 - recommended is 11 or later). For details about 
            what's included, and minimim requirements please see the
             [installation page](/docs/operations/installation).
          </MarkdownBlock>
          <h3 id="latest">Current version (Stable)</h3>
          <table className="versions">
            <tbody>
              <tr>
                <th>{latest.version}</th>
                <td>
                <a href={`${repoUrl}/releases/download/v${latest.version}/hazelcast-jet-${latest.version}.tar.gz`}>
                        hazelcast-jet-{latest.version}.tar.gz
                </a>
                </td>
                <td>{latest.size}MB</td>
                <td>
                  <a href={ latest.releaseNotes ? `${latest.releaseNotes}`
                  : `${repoUrl}/releases/tag/v${latest.version}`}>
                    Release Notes
                  </a>
                </td>
                <td>
                  <a
                    href={`/javadoc/${latest.version}`} target="_blank" rel="noreferrer noopener">
                    Javadoc
                  </a>
                </td>
              </tr>
            </tbody>
          </table>
          <p>Hazelcast Jet artifacts can also be retrieved using the following Maven coordinates:</p>
          
          <pre><code className="language-groovy css hljs">
              groupId: <span className="hljs-string">com.hazelcast.jet</span><br/>
              artifactId: <span className="hljs-string">hazelcast-jet</span><br/>
              version: <span className="hljs-string">{latest.version}</span>
         </code></pre>
         <p>For the full list of modules, please see <a href="https://search.maven.org/search?q=g:com.hazelcast.jet">Maven Central</a>.</p>
          <h3 id="archive">Past Versions</h3>
          <p>Here you can find previous versions of Hazelcast Jet.</p>
          <table className="versions">
            <tbody>
              {versions.map(
                current =>
                  current.version !== latest.version && (
                    <tr key={current.version}>
                      <th>{current.version}</th>
                      <td>
                      <a href={`${repoUrl}/releases/download/v${current.version}/hazelcast-jet-${current.version}.tar.gz`}>
                        hazelcast-jet-{current.version}.tar.gz
                      </a>
                      </td>
                      <td>
                      {current.size} MB
                      </td>
                      <td>
                      <a href={ current.releaseNotes ? `${current.releaseNotes}`
                        : `${repoUrl}/releases/tag/v${current.version}`}>
                          Release Notes
                        </a>
                      </td>
                      <td>
                        <a
                          href={`/javadoc/${current.version}`} target="_blank" rel="noreferrer noopener">
                          Javadoc
                        </a>
                      </td>
                    </tr>
                  ),
              )}
            </tbody>
          </table>
        </div>
      </Container>
    </div>
  );
}

module.exports = Downloads;
