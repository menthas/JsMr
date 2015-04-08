/**
 * Removes a client and cleanups related tasks
 * @param  {Object} client  The client to be removed
 * @param  {Object} runtime the runtime stats map
 * @return {Promise<undefined>}
 */
module.exports.removeClient = function (client, runtime) {
    // TODO remove active tasks, etc.
    runtime.client_count++;
    runtime.total_client_uptime += (client.last_activity - client.created_at) / 1000;

    return client.destroy();
}

/**
 * Load and configure an S3 SDK instance.
 * @return {Object} S3 SDK instance
 */
module.exports.getS3 = function () {
    var AWS = require('aws-sdk');
    AWS.config.loadFromPath(__dirname + "/../config/aws.json");
    return new AWS.S3();
}
