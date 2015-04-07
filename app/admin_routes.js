Sequelize = require('sequelize');
var utils = require('../lib/utils.js')();
var jobf = require('../lib/job.js');
var fs = require('fs');
var syntax_check = require('syntax-error');

var server = module.parent.exports.server;
var storage = module.parent.exports.storage;
var runtime = module.parent.exports.runtime;

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
            next();
         })
         .error(function () {
            res.json({error:true});
            next();
         });
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
            next();
         })
         .error(function() {
             res.json({error:"Failed to retrieve client list at this time."});
             next();
         });
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
            next();
         })
         .error(function() {
             res.json({error:"Failed to retrieve job list at this time."});
             next();
         });
});

/**
 * Create a new job from a .js file and the job attributes.
 */
server.post('/admin/job', function (req, res, next) {
    try {
        var err = syntax_check(fs.readFileSync(req.files.job_file.path));
        if (err) // make sure the code is syntax error free
            throw err;
        // TODO validate params and job file
        var job = require(req.files.job_file.path);
        var input = job.input;
        if (req.params.s3_input) {
            var parts = req.params.s3_input.split(":");
            if (parts.length != 2) {
                res.json({
                    error: "invalid S3 input"
                })
                next();
                return;
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
            req.files.job_file.path, job, db_params, storage
        );
        res.json({
            error:false
        });
        next();
    } catch (err) {
        res.json({
            error: "Can't load the Job file: " + err
        });
        next();
    }
});
