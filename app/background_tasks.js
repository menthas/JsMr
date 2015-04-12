var utils = require("../lib/utils.js")(conf);
var commons = require("../lib/commons.js");
var jobf = require('../lib/job.js');

var server = module.parent.exports.server;
var storage = module.parent.exports.storage;
var conf = module.parent.exports.conf;
var runtime = module.parent.exports.runtime;

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
    // END with inactive clients
    
    // progress jobs to next steps
    storage.Job.findAll({
        where: {
            completed: false,
            paused: false,
            error: null
        }
    }).then(function (jobs) {
        if (!jobs)
            return;
        jobs.forEach(function (job) {
            storage.Task.count({
                where: {
                    job_id: jobs[i].id,
                    step: jobs[i].current_step,
                    completed: false,
                }
            }).then(function (c) {
                if (c == 0) {
                    jobf.compactStep(job, conf).then(function (new_input_files) {
                        var job_info = jobf.getJobInfo(job.id);
                        if (job_info.chain.length > job.current_step + 1) {
                            job.current_step += 1;
                            job.save();
                            jobf.addTasks(job_info, job, new_input_files,
                                          storage, conf.get('default_split_size'));
                        } else {
                            jobf.finish(job);
                        }
                    })
                    .error(function(err) {
                        job_entry.error = err;
                        job_entry.paused = true;
                        job_entry.save();
                    });
                }
            });
        });  
    });
    // END progress jobs

}, conf.get('cleanup_task_interval'));
