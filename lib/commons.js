var AWS = require('aws-sdk');
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
    //TODO handle error case if the file does not exists
    AWS.config.loadFromPath(__dirname + "/../config/aws.json");
    return new AWS.S3();
}


/**
 * Load and return confif
 * @return {object} AWS Config
 */
module.exports.getConfig = AWS.config;

