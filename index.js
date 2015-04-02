// First load the configurations
var nconf = require('./config');
module.exports.conf = nconf;

// Create and Configure the server
var restify = require('restify'),
    os = require('os');
var server = restify.createServer({
    name: 'JsMr'
});

server.use(restify.acceptParser(server.acceptable));
server.use(restify.queryParser());
server.use(restify.bodyParser({
    maxBodySize: nconf.get('max_client_load'), // 10MB max
    mapParams: true,
    mapFiles: false,
    overrideParams: true,
    keepExtensions: false,
    uploadDir: os.tmpdir(),
    multiples: true
}));
server.use(restify.gzipResponse());
server.get(/\/static\/?.*/, restify.serveStatic({
    directory: './public'
}));

server.listen(nconf.get('port'), function () {
    console.log('%s listening at %s', server.name, server.url);
});
module.exports.server = server;
// Done with sever configuration

// Load and configure storage
module.exports.storage = require('./app/storage.js');

// Load routes
var routes = require('./app/routes.js');
var admin_routes = require('./app/admin_routes.js');

// Setup Background tasks
var bg_tasks = require('./app/background_tasks.js');

// Dummy home page - do something else or remove altogether
server.get('/', function (req, res, next) {
    res.json({ hello: "world !" });
    return next();
});