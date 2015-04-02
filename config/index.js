var nconf = require('nconf');

// load the configuration in the following order:
//   1. command line arguments: --foo=bar
//   2. enviromental variables: NODE_ENV=...
//   3. app.json config file
nconf.argv()
     .env()
     .file({ file: __dirname + '/app.json' });

module.exports = nconf;
