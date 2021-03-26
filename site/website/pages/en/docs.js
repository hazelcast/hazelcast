const React = require("react");
const Redirect = require("../../core/Redirect.js");

const siteConfig = require(process.cwd() + "/siteConfig.js");

function docUrl(doc, language) {
  return (
    siteConfig.baseUrl +
    "docs/" +
    (language ? language + "/" : "") +
    doc 
  );
}

class Docs extends React.Component {
  render() {
    return (
      <Redirect
        redirect={docUrl("get-started/intro", this.props.language)}
        config={siteConfig}
      />
    );
  }
}

module.exports = Docs;