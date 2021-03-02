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
const modules = require(`${CWD}/modules.json`);
const incubatingModules = require(`${CWD}/incubating-modules.json`);
const versions = require(`${CWD}/all-versions.json`);
const MarkdownBlock = CompLibrary.MarkdownBlock;

function Downloads(props) {
  const { config: siteConfig } = props;
  const latest = versions[0];
  const repoUrl = `https://github.com/${siteConfig.organizationName}/${siteConfig.projectName}`;
  const contribRepoUrl = `https://github.com/${siteConfig.organizationName}/${siteConfig.projectName}-contrib`
  return (
    <div className="docMainWrapper wrapper">
      <Container className="mainContainer versionsContainer">
        <div>
          <header>
            <h1>{siteConfig.title} Downloads</h1>
          </header>
          <MarkdownBlock>
            There are two different Hazelcast Jet download packages - regular and slim.
            The regular Hazelcast Jet download package includes Hazelcast Jet server and
            all production ready modules. The slim package includes only Hazelcast Jet server
            and all modules need to be downloaded separately (see [Modules](#modules) section below).
            Both require a JDK to run, which can be obtained from
            [AdoptOpenJDK](https://adoptopenjdk.net) (minimum version is 8 - recommended is 11 or later).
            For details about what's included, and minimum requirements please see the
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
                <td>{latest.size} MB</td>
                <td>
                  <a href={latest.releaseNotes ? `${latest.releaseNotes}`
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
              <tr>
                <th>{latest.version}-slim</th>
                <td>
                  <a href={`${repoUrl}/releases/download/v${latest.version}/hazelcast-jet-${latest.version}-slim.tar.gz`}>
                    hazelcast-jet-{latest.version}-slim.tar.gz
                </a>
                </td>
                <td>{latest.slimSize} MB</td>
                <td>
                  <a href={latest.releaseNotes ? `${latest.releaseNotes}`
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
            groupId: <span className="hljs-string">com.hazelcast.jet</span><br />
              artifactId: <span className="hljs-string">hazelcast-jet</span><br />
              version: <span className="hljs-string">{latest.version}</span>
          </code></pre>

          <h3 id="incubating-modules">Additional Modules</h3>
          <p>The following modules are part of the regular Jet distribution. You can download each module separately
          to use it with the slim distribution.</p>

          <table className="modules">
            <thead>
              <tr>
                <th>Name</th>
                <th>File</th>
                <th>Size</th>
              </tr>
            </thead>
            <tbody>
              {modules.map(
                  module =>
                      <tr key={module.name}>
                        <td>
                          <a href={module.docs}>
                            {module.name}
                          </a>
                        </td>
                        <td>
                          <a href={module.download}>
                            {module.filename}
                          </a>
                        </td>
                        <td>
                          {module.size} MB
                        </td>
                      </tr>
              )}
            </tbody>
          </table>

          <h3 id="incubating-modules">Incubating Modules</h3>
          <p>In addition to the ones included in the main distribution, Jet also has
          the following modules:
             </p>
          <table className="modules">
            <thead>
              <tr>
                <th>Name</th>
                <th>File</th>
                <th>Size</th>
              </tr>
            </thead>
            <tbody>
              {incubatingModules.map(
                module =>
                  <tr key={module.name}>
                    <td>
                      <a href={module.docs}>
                        {module.name}
                      </a>
                     </td>
                    <td>
                      <a href={module.download}>
                        {module.filename}
                      </a>
                    </td>
                    <td>
                      {module.size} MB
                    </td>
                  </tr>
              )}
            </tbody>
          </table>
          <p>You can include them in the classpath by moving the downloaded jars to the lib folder under your Jet Home. </p>
          <p>For the full list of modules, please see <a href="https://search.maven.org/search?q=g:com.hazelcast.jet">Maven Central</a>.</p>

        <h3 id="management-center">Management Center</h3>

         <p>
           Hazelcast Management Center can be used for monitoring the cluster.
         </p>

         <p>
          Download <a href={`https://download.hazelcast.com/management-center/hazelcast-management-center-${latest.mcVersion}.tar.gz`}>Hazelcast Management Center {latest.mcVersion}</a>
           &nbsp;(compatible wit Hazelcast Jet {latest.version}).
         </p>
         <p>
           See the reference Hazelcast Management Center <a href={`https://docs.hazelcast.com/management-center/${latest.mcVersion}/index.html`}>documentation</a>.
          </p>
         <p>
          For instructions on how to download and run Hazelcast Jet Management Center (compatible with Hazelcast Jet 4.3 and earlier),
          see the <a href="/docs/4.3/enterprise/management-center">installation page</a>.
         </p>

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
                        <a href={current.releaseNotes ? `${current.releaseNotes}`
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
                      <td>
                        <a
                          href={`https://download.hazelcast.com/hazelcast-jet-management-center/hazelcast-jet-management-center-${current.version}.tar.gz`} target="_blank" rel="noreferrer noopener">
                          Management Center
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
