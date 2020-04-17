const replace = require('replace-in-file');
const options = {
  files: "../docs/**/**.md",
  from: [/{jet-version}/g, /{imdg-version}/g],
  to: ['4.0', '4.0'],
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

