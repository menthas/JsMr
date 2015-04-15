var fs = require('fs');
var path = require('path');

var commons = require('../lib/commons.js');
var job = require('../lib/job.js');
var server = module.parent.exports.server;
var storage = module.parent.exports.storage;
var runtime = module.parent.exports.runtime;
var Sequelize = require('sequelize');

/**
 * A task response is:
 *  + instance_id
 *  + stage
 *  + job_id
 *  + task_id
 *  + start_index
 *  + end_index
 *  + bucket_name
 *  + object_key
 *  + output_bucket_name
 *  + access_key
 *  + secret_key
 *  + config
 */

/**
 * (Un)Register a client
 */
server.post('/register', function registerHandler(req, res, next) {
    /**
     * @param  {string} auth_token   client auth token used to stablish a valid client
     * @param  {string} client_id    only present for unregister
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
            job.schedule(new_user,storage,res);
            console.log('Client created');
            next();

        });
    } else if (req.params.action == 'unregister') { // unregister client
        storage.Client.find(req.params.client_id).then(function (client) {
            if (client != null) {
                commons.removeClient(client, runtime).then(function () {
                        res.json({
                            unregistered: true
                        });
                });
            } else {
                res.json({
                    unregistered: true
                });
            }
        })
    }
    return next();
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
    });
    return next();
});


/**
 * Update the task in the following scenarios :
 * a) Task is successfully finished by the client
 * b) Run Task Failure by client.
 * c) Client closed during task run.
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
    storage.Task.findOne({
        where: {
            id: req.params.task_id
        }
    }).then(function (task) {

        if(req.params.action == 'task_success')
        {
            task.completed = true;
        }
        if(req.params.action == 'task_failure')
        {
            task.failed = task.failed + 1;
        }
        task.taken = 0;
        task.save();
        res.json({
            task: null
        });

    });
    return next();
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

/**
 * Get the code for a job's stage (mapper/reducer in stage `i`).
 * NOTE: the result is a JSONP string which means it will add a function with
 *       the name `callback` when loaded on the client side.
 */
server.get('/code', function codeGetHandler(req, res, next) {
    /**
     * @param  {string}  auth_token
     * @param  {string}  client_id
     * @param  {string}  job_id
     * @param  {integer} stage
     * @param  {string}  return_func
     * @return bulk of code to run
     */
    storage.Client.find({
        where: {
            //auth_token: req.params.auth_token,
            id: req.params.client_id
        }
    }).then(function (client) {
        if (!client)
            return res.send(404, "Client not found");
        var job_id = req.params.job_id.replace(/[^0-9a-z-]+/g, '');
        var job_file = path.normalize(__dirname+'/../jobs/'+job_id+'.job')
        if (fs.existsSync(job_file)) {
            try {
                job_info = require(job_file);
                job_function = job_info.chain[parseInt(req.params.step)].toString();
                res.contentType = 'application/javascript';
                return res.send(200, "var " + req.params.return_func + " = " + job_function);
            } catch(err) {
                return res.send(500, "Could not find/load the requested job.");
            }
        }
    });
    return next();
});
