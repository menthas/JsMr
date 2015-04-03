var server = module.parent.exports.server;
var storage = module.parent.exports.storage;
var conf = module.parent.exports.conf;
var runtime = module.parent.exports.runtime;
var utils = require("../lib/utils.js")(conf);
var commons = require("../lib/commons.js");

/**
 * Setup a cleanup task to do the following:
 *   1. Cleanup inactive clients and release their tasks
 *   2. ...
 */
setInterval(function() {
    utils.log("Cleanup Task: started", "info");

    // Cleanup inactive clients
    var timeout_time = new Date();
    timeout_time.setSeconds(timeout_time.getSeconds() - conf.get("client_timeout"));
    storage.Client.findAll({
        where: {
            last_activity: {
                $lt: timeout_time
            }
        }
    }).then(function (clients) {
        if (!clients)
            return;
        utils.log("Found " + clients.length + " inactive clients. removing ...");
        for (var i=0; i<clients.length; i++) {
            commons.removeClient(clients[i], runtime);
        }
    });
    // DONE with inactive clients

}, conf.get('cleanup_task_interval'));
