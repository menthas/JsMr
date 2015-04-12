var Sequelize = require('sequelize');
var fs = require('fs');
var syntax_check = require('syntax-error');

var utils = require('../lib/utils.js')();
var jobf = require('../lib/job.js');
var commons = require('../lib/commons.js');

var server = module.parent.exports.server;
var storage = module.parent.exports.storage;
var runtime = module.parent.exports.runtime;
var conf = module.parent.exports.conf;

/**
 * Returns information used for the admin dashboard page.
 */
server.get('/admin/dashboard', function (req, res, next) {
    var chain = new Sequelize.Utils.QueryChainer();
    chain.add(storage.Client.count())
         .add(storage.Job.count())
         .run()
         .success(function (result) {
            var avg_uptime = runtime.client_count > 0 ?
                runtime.total_client_uptime / runtime.client_count : 0;
            res.json({
                'clients': result[0],
                'jobs': result[1],
                'avg_uptime': avg_uptime,
                'total_clients': runtime.client_count,
                'uptime': Math.floor((new Date() - runtime.uptime) / 1000)
            });
         })
         .error(function () {
            res.json({error:true});
         });
    return next();
});

/**
 * Returns a list of clients for the DataTable plugin
 */
server.get('/admin/clients', function (req, res, next) {
    var chain = new Sequelize.Utils.QueryChainer();
    var find_params = {
        offset: parseInt(req.params.start),
        limit: req.params.length,
    }
    if (req.params.search.value) {
        find_params.where = {
            id: req.params.search.value
        }
    }
    var find = storage.Client.findAll(find_params);
    var client_to_array = function (client) {
        var failed_pr = client.tasks_failed > 0 ?
            Math.floor(client.tasks_failed / (client.tasks_done + client.tasks_failed) * 100) : 0;
        var uptime = Math.floor((client.last_activity - client.created_at) / 1000);
        return [
            client.id,
            client.busy ? "Working" : "Idle",
            client.tasks_done,
            failed_pr + "%",
            utils.time_string(uptime),
            client.last_activity.toLocaleString(),
        ];
    };
    chain.add(storage.Client.count())
         .add(find)
         .run()
         .success(function (result) {
            res.json({
                draw: req.params.draw,
                recordsTotal: result[0],
                recordsFiltered: result[0],
                data: result[1] ? result[1].map(client_to_array) : []
            });
         })
         .error(function() {
             res.json({error:"Failed to retrieve client list at this time."});
         });
    return next();
});

/**
 * Returns a list of jobs for the DataTable plugin
 */
server.get('/admin/jobs', function (req, res, next) {
    var chain = new Sequelize.Utils.QueryChainer();
    var find_params = {
        offset: parseInt(req.params.start),
        limit: req.params.length,
    }
    if (req.params.search.value) {
        find_params.where = {
            $or: [
                { id: req.params.search.value },
                { name: { $like: '%' + req.params.search.value + '%' } }
            ]
        }
    }
    var find = storage.Job.findAll(find_params);
    var job_to_array = function (job) {
        return [
            job.id,
            job.name ? job.name : "N/A",
            job.completed ? "Yes" : "No",
            job.paused ? "Yes" : "No",
            job.current_step + 1,
            job.error ? job.error : "None",
            job.input_file,
            job.output_dir,
        ];
    };
    chain.add(storage.Job.count())
         .add(find)
         .run()
         .success(function (result) {
            res.json({
                draw: req.params.draw,
                recordsTotal: result[0],
                recordsFiltered: result[0],
                data: result[1] ? result[1].map(job_to_array) : []
            });
         })
         .error(function() {
             res.json({error:"Failed to retrieve job list at this time."});
         });
    return next();
});

/**
 * Create a new job from a .js file and the job attributes.
 */
server.post('/admin/job', function (req, res, next) {
    var err = syntax_check(fs.readFileSync(req.files.job_file.path));
    if (err) { // make sure the code is syntax error free
        res.json({
            error: "Can't load the Job file: " + err
        });
        return next();
    }

    var job = require(req.files.job_file.path);

    var err = _validateJob(job);
    if (err) {
        res.json({
            error: err
        });
        return next();
    }

    var input = job.input;
    if (req.params.s3_input) {
        var parts = req.params.s3_input.split(":");
        if (parts.length != 2) {
            res.json({
                error: "invalid S3 input"
            })
            return next();
        }
        input = {
            type: 'AWS',
            bucket: parts[0],
            key: parts[1]
        }
    }
    var output = job.output;
    if (req.params.s3_output) {
        output = {
            type: 'AWS',
            bucket: req.params.s3_output
        }
    }
    var db_params = {
        input: input,
        output: output,
        paused: req.params.unpause != '1',
        name: req.params.job_name ? req.params.job_name : "JsMr Job"
    }
    // add the job and possibly schedule it.
    jobf.add(
        req.files.job_file.path, job, db_params, storage, conf
    );
    res.json({
        error:false
    });
    return next();
});

/**
 * Validate a 
 * @param  {Object}  job  The executed job file
 * @return {String|undefined}  A string with an error message or undefined if
 *         job is valid.
 */
function _validateJob(job) {
    if (job.chain.length == 0) {
        return "The specified job doen't include any map/reduce steps.";
    } else {
        for (var i=0; i<job.chain.length; i++) {
            if (typeof job.chain[i] !== 'function') {
                return "Map/Reduce at step "+(i+1)+" is not a valid function.";
            } else {
                try {
                    var step = job.chain[i]();
                    if (typeof step.instances !== 'number' || step.instances == 0)
                        return "Map/Reduce at step "+(i+1)+" doesn't have a valid number of instances";
                } catch (err) {
                    return "Map/Reduce at step "+(i+1)+" can't be called.\nError" + err;
                }
            }
        }
    }
}
