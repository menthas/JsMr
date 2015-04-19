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
            if (clients[i].task_id) {
                storage.Task.find(clients[i].task_id).then(function (task) {
                    task.taken = 0;
                    task.client_id = null;
                    if (task.instance != null) {
                        var inst_key = task.job_id + "_" + task.step;
                        jobf.instanceInfo[inst_key].push(task.instance);
                    }
                    task.save();
                });
            }
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
        utils.log("BG Task: got " + jobs.length + " to check");
        jobs.forEach(function (job) {
            storage.Task.count({
                where: {
                    job_id: job.id,
                    step: job.current_step,
                    completed: false,
                }
            }).then(function (c) {
                if (c == 0) {
                    utils.log("BG Task: job " + job.id + " needs to be progressed");
                    jobf.compactStep(job, conf, storage).then(function (new_input_files) {
                        var job_info = jobf.getJobInfo(job.id);
                        if (job_info.chain.length > job.current_step + 1) {
                            utils.log(job.id + ": Compaction complete. Creating new tasks");
                            job.current_step += 1;
                            job.save();
                            jobf.addTasks(job_info, job, new_input_files,
                                          storage, conf.get('default_split_size'));
                        } else {
                            utils.log(job.id + ": Compaction complete. finishing the job");
                            jobf.finish(job);
                        }
                    })
                    .catch(function(err) {
                        utils.log(job.id + ": Compaction failed!", utils.ll.ERROR);
                        job.error = JSON.stringify(err);
                        job.paused = true;
                        job.save();
                    });
                }
            });
        });  
    });
    // END progress jobs

}, conf.get('cleanup_task_interval'));
