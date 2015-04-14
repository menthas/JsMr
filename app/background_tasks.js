var utils = require("../lib/utils.js");
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
    utils.log("BG Task: started");

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
        if (!clients.length)
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
        if (!jobs.length)
            return;
        utils.log("BG Task: got " + jobs.count + " to check");
        jobs.forEach(function (job) {
            storage.Task.count({
                where: {
                    job_id: jobs[i].id,
                    step: jobs[i].current_step,
                    completed: false,
                }
            }).then(function (c) {
                if (c == 0) {
                    utils.log("BG Task: job " + job.id + " needs to be progressed");
                    jobf.compactStep(job, conf).then(function (new_input_files) {
                        utils.log(job_entry.id + ": Compaction complete. Creating new tasks");
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
                        utils.log(job.id + ": Compaction failed!", utils.ll.ERROR);
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
