var server = module.parent.exports.server;
var storage = module.parent.exports.storage;

/**
 * (Un)Register a client
 */
server.post('/register', function registerHandler(req, res, next) {
    /**
     * @param  {string} auth_token   client auth token used to stablish a valid client
     * @param  {string} action       register|unregister
     * @param  {string} agent        Browser agent identifier
     * @return {
     *         registered: true|false,
     *         unregistered: true|false,
     *         client_id: string,
     *         task: Task
     * }
     */
    if (req.params.action == 'register') { // register client
        var new_client = storage.Client.create({
            auth_token: req.params.auth_token,
            busy: false,
            agent: req.params.agent,
            tasks_done: 0,
            tasks_failed: 0,
            last_activity: new Date(),
            prev_jobs: [],
        }).then(function (new_user) {
            res.json({
                registered: true,
                client_id: new_user.id,
                task: null
            });
            next();
        }).catch(function (error) {
            res.json({
                registered: false,
            });
            next();
        });
    } else { // unregister client
        next();
    }
});

/**
 * Heartbeat sent by client in case there's no activity for `n` seconds.
 */
server.get('/beat', function beatHandler(req, res, next) {
    /**
     * @param  {string} auth_token   client auth token used to stablish a valid client
     * @param  {string} client_id
     * @param  {string} task_id
     * @param  {string} job_id
     * @return {
     *         valid: true|false,
     *         new_task: null|Task
     * }
     */
    var client_id = req.params.client_id,
        auth_token = req.params.auth_token;
    storage.Client.find({
        where: {
            id: client_id, auth_token: auth_token
        }
    }).then(function (client) {
        if (client == null) {
            res.json(404, {
                error_msg: 'Client not found'
            });
        } else {
            res.json({
                valid: true,
                new_task: null
            });
            client.last_activity = new Date();
            client.save();
        }
        next();
    });
});

/**
 * Task Updates
 */
server.post('/task', function taskPostHandler(req, res, next) {
    /**
     * @param  {string}  auth_token
     * @param  {string}  client_id
     * @param  {string}  task_id
     * @param  {string}  job_id
     * @param  {int}     records_consumed
     * @param  {boolean} success
     * @param  {boolean} need_chunk
     * @param  {list}    output
     * @param  {Object}  state
     * @param  {float}   elapsed_time
     * @return {
     *         task: null|Task
     * }
     */
});

/**
 * Get new task; used by an existing client that for some reason abandoned a task.
 * This will result in any current tasks to be marked as failed
 */
server.get('/task', function taskGetHandler(req, res, next) {
     /**
     * @param  {string}  auth_token
     * @param  {string}  client_id
     * @return {
     *         task: null|Task
     * }
     */
});

/**
 * AUX1. Request the next data chunk to speedup the process
 */
server.get('/chunk', function chunkGetHandler(req, res, next) {
     /**
     * @param  {string}  auth_token
     * @param  {string}  client_id
     * @param  {string}  task_id
     * @param  {string}  job_id
     * @return {
     *         task: null|Task
     * }
     */
});
