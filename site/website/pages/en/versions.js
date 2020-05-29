/**
 * Copyright (c) 2017-present, Facebook, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

const React = require("react");

const CompLibrary = require("../../core/CompLibrary");

const Container = CompLibrary.Container;

const CWD = process.cwd();

const versions = require(`${CWD}/all-versions.json`);

function Versions(props) {
  const { config: siteConfig } = props;
  const latest = versions[0];
  const repoUrl = `https://github.com/${siteConfig.organizationName}/${siteConfig.projectName}`;
  return (
    <div className="docMainWrapper wrapper">
      <Container className="mainContainer versionsContainer">
        <div className="post">
          <header className="postHeader">
            <h1>{siteConfig.title} Documentation Versions</h1>
          </header>
          <h3 id="latest">Current version (Stable)</h3>
          <table className="versions">
            <tbody>
              <tr>
                <th>{latest.version}</th>
                <td>
                  {/* You are supposed to change this href where appropriate
                        Example: href="<baseUrl>/docs(/:language)/:id" */}
                  <a
                    href={`${siteConfig.baseUrl}${siteConfig.docsUrl}/${
                      props.language ? props.language + "/" : ""
                    }get-started/intro`}
                  >
                    Documentation
                  </a>
                </td>
                <td>
                  <a href={ latest.releaseNotes ? `${latest.releaseNotes}`
                  : `${repoUrl}/releases/tag/v${latest.version}`}>
                    Release Notes
                  </a>
                </td>
                <td>
                  <a href={`/javadoc/${latest.version}`} target="_blank" rel="noreferrer noopener">Javadoc</a>
                </td>
              </tr>
            </tbody>
          </table>
          <h3 id="rc">Pre-release versions</h3>
          <table className="versions">
            <tbody>
              <tr>
                <th>master</th>
                <td>
                  {/* You are supposed to change this href where appropriate
                        Example: href="<baseUrl>/docs(/:language)/next/:id" */}
                  <a
                    href={`${siteConfig.baseUrl}${siteConfig.docsUrl}/${
                      props.language ? props.language + "/" : ""
                    }next/get-started/intro`}
                  >
                    Documentation
                  </a>
                </td>
                <td>
                  <a href={repoUrl} target="_blank" rel="noreferrer noopener">Source Code</a>
                </td>
              </tr>
            </tbody>
          </table>
          <p>Latest development documentation.</p>
          <h3 id="archive">Past Versions</h3>
          <p>Here you can find previous versions of the documentation.</p>
          <table className="versions">
            <tbody>
              {versions.map(
                current =>
                current !== latest && (
                    <tr key={current.version}>
                      <th>{current.version}</th>
                      <td>
                        {/* You are supposed to change this href where appropriate
                        Example: href="<baseUrl>/docs(/:language)/:version/:id" */}
                        <a href={ current.manual ? `${current.manual}` 
                          : `${siteConfig.baseUrl}${siteConfig.docsUrl}/${props.language ? props.language + "/" : ""}${current.version}/get-started/intro`
                        }>
                          Documentation
                        </a>
                      </td>
                      <td>
                      <a href={ current.releaseNotes ? `${current.releaseNotes}`
                      : `${repoUrl}/releases/tag/v${current.version}`}>
                        Release Notes
                      </a>
                      </td>
                      <td>
                        <a href={`/javadoc/${current.version}`} target="_blank" rel="noreferrer noopener">Javadoc</a>
                      </td>
                    </tr>
                  )
              )}
            </tbody>
          </table>
        </div>
      </Container>
    </div>
  );
}

module.exports = Versions;
