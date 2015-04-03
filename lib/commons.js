module.exports.removeClient = function (client, runtime) {
    // TODO remove active tasks, etc.
    runtime.client_count++;
    runtime.total_client_uptime += (client.last_activity - client.created_at) / 1000;

    return client.destroy();
}
