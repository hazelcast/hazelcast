const replace = require('replace-in-file');
const fs = require('fs');

let data = JSON.parse(fs.readFileSync('attributes.json'));
console.log("Using attributes", data);

let from = Object.keys(data).map(k => new RegExp(`{${k}}`, 'g'));
let to = Object.values(data);

const options = {
  files: "../docs/**/**.md",
  from: from,
  to: to,
  isRegex: true,
  countMatches: true
};

try {
  const results = replace.sync(options).filter(result => result.hasChanged)
  console.log('Replacement results:', results);
}
catch (error) {
  console.error('Error occurred:', error);
}

