var mv = require('mv');
var path = require('path');
var Sequelize = require('sequelize');
var Promise = require('bluebird');
var crypto = require('crypto');

var utils = require("../lib/utils.js");
var commons = require('../lib/commons.js');

var s3 = commons.getS3();

/**
 * A Map which stores the number of instances for
 * each step
 */
var instanceInfo = {};

module.exports.instanceInfo = instanceInfo;

/**
 * Load and return the job file, The file should exist and be valid at this point
 * @param  {string} job_id UUID for the job to load
 * @return {Object}        Object containing job code and information
 */
module.exports.getJobInfo = function (job_id) {
    return require(path.normalize(__dirname+'/../jobs/'+job_id+'.job'))
};

/**
 * Add a new job to the queue
 * @param {string} file_path temporary path of the job .js file
 * @param {object} info      the loaded job object
 * @param {object} db_params parameters that are required for the DB
 * @param {object} storage   pointer to the database ORM
 */
module.exports.add = function (file_path, info, db_params, storage, conf) {
    var db_input, db_output;
    if (db_params.input.type == 'AWS') {
        db_input = 'AWS:' + db_params.input.bucket + ':' + db_params.input.key;
    } else {
        db_input = 'LOCAL:' + db_params.input.path;
    }
    if (db_params.output.type == 'AWS') {
        db_output = 'AWS:' + db_params.output.bucket;
    } else {
        db_output = 'LOCAL:' + db_params.output.path;
    }
    storage.Job.create({
        name: db_params.name,
        current_step: 0,
        input_file: db_input,
        output_dir: db_output,
        completed: false,
        paused: db_params.paused,
        error: null
    }).then(function (new_job) {
        utils.log(new_job.id + ": Job DB entry created");
        mv(file_path, path.normalize(__dirname+'/../jobs/'+new_job.id+'.job'),
            function (err) {
                if (err) {
                    utils.log(new_job.id + ": Failed to move job file (mv) " + err, utils.ll.ERROR);
                    // TODO handle failed mv
                }
            }
        );
        module.exports.addTasks(
            info, new_job, [new_job.input_file],
            storage, conf.get('default_split_size')
        );
    });
};

/**
 * Add all tasks for the new job
 * @param {object} info     Job object loaded from the .js file
 * @param {object} db_entry DB entry representing this job
 * @param {object} storage  pointer to the DB ORM
 */
module.exports.addTasks = function(info, db_entry, input_sources, storage, split_size) {
    var step_func = info.chain[db_entry.current_step]();
    var instances = step_func.instances;
    var is_reduce = step_func.is_reduce;

    utils.log(db_entry.id + ": Creating tasks, reduce: " + (is_reduce ? "Yes" : "No") +
        ", instances: " + instances + ", # inputs:" + input_sources.length);

    input_sources.forEach(function (input_source, index) {
        var input = input_source.split(":");
        // Check to see if the input exists on AWS
        if (input[0] == 'AWS') {
            commons.S3Head(s3, input[1], input[2]).then(function (data) {
                instance = undefined;
                if (is_reduce) {
                    instance = (instances == -1 ? index : ((index % instances) + 1));
                }
                _addTasksWithLen(
                        info, db_entry, input_source, storage,
                        data.ContentLength, split_size, instance);
            }).error(function (err) {
                utils.log(db_entry.id + ": Failed to find object on AWS", utils.ll.ERROR);
                db_entry.error = err.code;
                db_entry.paused = true;
                db_entry.save();
            });
        } else {
            // TODO add local storage support
            db_entry.error = "None AWS storage in not supported (addTasks)";
            db_entry.paused = true;
            db_entry.save();
        }
    });
};

/**
 * Add all the necessary tasks for a job
 * @param {object} info       Job object loaded from the .js file
 * @param {object} db_entry   DB entry representing this job
 * @param {object} storage    pointer to the DB ORM
 * @param {int}    len        The length of the input file
 * @param {int}    split_size The split size of the input file
 */
function _addTasksWithLen(info, db_entry, input_source, storage, len, split_size, instance) {
    var chain = new Sequelize.Utils.QueryChainer();
    var task_count = Math.ceil(len / split_size);
    var cur_step_func = info.chain[db_entry.current_step]();
    var instances = cur_step_func.instances;
    var needs_sort = info.chain.length > db_entry.current_step + 1 &&
        (info.chain[db_entry.current_step + 1]()).is_reduce;

    utils.log(db_entry.id + ": Creating " + task_count + " tasks with size " +
        utils.size_string(split_size));

    // populate the instance tickets for this job
    var instance_key = db_entry.id.concat('_',db_entry.current_step);
    if (instance !== undefined) {
        if (instanceInfo[instance_key] === undefined)
            instanceInfo[instance_key] = [];
        instanceInfo[instance_key].push(instance);
    } else {
        var no_of_instances = [];
        for(var n = 0; n < instances ; n++)
            no_of_instances.push((n % instances) + 1);
        instanceInfo[instance_key] = no_of_instances;
    }

    // TODO change the insert to be a single insert instead (or batched at least)
    for (var i = 0; i<task_count; i++) {
        var task_instance = instance === undefined ? 
            (instances == -1 ? null : ((i % instances) + 1)) : instance;
        var params = {
            failed: 0, attempts: 0, replicates: 1, taken: 0, step: db_entry.current_step,
            completed: false,
            input_file: input_source,
            input_offset: i*split_size,
            input_size: split_size,
            instance: task_instance,
            sort: needs_sort,
            job_id:db_entry.id
        };
        chain.add(storage.Task.create(params));
    }
    chain.run().then(function () {
        utils.log(db_entry.id + ": All tasks created");
    }).catch(function() {
        utils.log(db_entry.id + ": Failed to create all tasks", utils.ll.ERROR);
        db_entry.paused = true;
        db_entry.error = "Wasn't able to create all tasks for step "+(db_entry.current_step+1);
        db_entry.save();
    });
}

module.exports.compactStep = function(job_entry, conf, storage) {
    utils.log(job_entry.id + ": started compaction for step "+job_entry.current_step);
    return storage.Task.findAll({
        where: {
            job_id: job_entry.id,
            step: job_entry.current_step,
            completed: true
        }
    }).then(function (tasks) {
        if (!tasks.length)
            return utils.log(job_entry.id + ": No completed tasks. zombie job?", utils.ll.WARNING);
        var need_sort = tasks[0].sort;
        if (need_sort)
            return _shuffleAndCompact(job_entry, tasks, conf.get("server_split_size"));
        else
            return _compact(job_entry, tasks);
    });
};

function _compact(job_entry, tasks) {
    utils.log(job_entry.id + ": Concat compaction (no reducer)");
    return new Promise(function (resolve, reject) {
        var output = job_entry.output_dir.split(":")
        if (output[0] != 'AWS')
            return reject("None AWS outputs are not supported.");

        var bucket = output[1];
        var key_base = job_entry.id + '/_temp';
        var key = key_base + '/step_' + job_entry.current_step + '_final';
        s3.createMultipartUpload({
            Bucket: bucket,
            Key: key
        }, function (err, data) {
            if (err)
                return reject(
                    "Failed to allocate space for compaction of step " +
                    job_entry.current_step
                );

            var total = tasks.length;
            var failed = false;
            var parts = []
            tasks.forEach(function (task, index) {
                s3.uploadPartCopy({
                    Bucket: bucket, Key: key, PartNumber: index + 1,
                    UploadId: data.UploadId,
                    CopySource: bucket + '/' + key_base + '/' + task.id
                }, function (err, copy_data) {
                    if (err) {
                        utils.log(job_entry.id + ": Failed to copy AWS part", utils.ll.ERROR);
                        failed = true;
                    } else
                        parts[index] = copy_data.CopyPartResult.ETag;
                    total -= 1;
                    if (total == 0) { // all requests are done, cleanup
                        if (failed) {
                            s3.abortMultipartUpload({
                                Bucket: bucket, Key: key, UploadId: data.UploadId
                            }).send();
                            return reject("Failed to compact task at step " + job_entry.current_step);
                        } else {
                            var part_data = [];
                            for (var i=0; i<parts.length; i++) {
                                part_data[i] = {
                                    ETag: parts[i],
                                    PartNumber: i + 1
                                }
                            }
                            console.log(part_data);
                            s3.completeMultipartUpload({
                                Bucket: bucket, Key: key, UploadId: data.UploadId,
                                MultipartUpload: { Parts: part_data }
                            }, function (err, data) {
                                if (err) {
                                    utils.log(job_entry.id + ": Failed to complete compaction",
                                        utils.ll.ERROR);
                                } else {
                                    utils.log(job_entry.id + ": Moved all chunks to the new file, " +
                                        "Cleaning up");
                                    for (var j=0; j<tasks.length; j++) {
                                        s3.deleteObject({
                                            Bucket: bucket, Key: key_base + '/' + tasks[j].id
                                        }).send();
                                        tasks[j].destroy();
                                        resolve(['AWS:' + bucket + ':' + key]);
                                    }
                                }
                            });
                        }
                    }
                    utils.log(job_entry.id + ": Concat Complete !");
                });
            });
        });
    });
}

function _shuffleAndCompact(job_entry, tasks, download_size) {
    utils.log(job_entry.id + ": Shuffle Compaction (reducer next)");
    return new Promise(function (resolve, reject) {
        var output = job_entry.output_dir.split(":")
        if (output[0] != 'AWS')
            return reject("None AWS outputs are not supported.");

        var bucket = output[1];
        var key_base = job_entry.id + '/_temp';
        var total_tasks = tasks.length;
        var total_tasks_copy = tasks.length;
        tasks.forEach(function (task) {
            var key_map = {};
            var num_keys = 0;
            s3.getObject({Bucket: bucket, Key: key_base + '/' + task.id}).
            on('httpData', function (chunk) {
                var lines = chunk.split("\n");
                var offset = 0;
                var old_key = null;
                var key = null;
                for (var i=0; i<lines.length; i++) {
                    key = lines[i].split(":")[0];
                    if (key !== old_key) {
                        if (old_key != null) // add bound for the old key
                            key_map[old_key][key_map.length-1].push(offset);
                        // add the new key
                        if (!(key in key_map)) {
                            key_map[key] = [];
                            num_keys++;
                        }
                        key_map[key].push([task.id, offset]);
                        old_key = key;
                    }
                    offset += len(lines[i]) + 1;
                }
                if (key != null) // add bound for the last key
                    key_map[key][key_map.length-1].push(offset);
            }).
            on('httpDone', function () {
                utils.log(job_entry.id + ": Parse complete. " +
                          total_tasks + "/" + total_tasks_copy + " remaining");
                total_tasks -= 1;
                if (total_tasks == 0) { // once all tasks are done
                    utils.log(job_entry.id + ": Parse Complete, creating " + num_keys +
                        " new inputs (for each output key)");
                    _createReduceChunks(job_entry, tasks, key_map, bucket,
                        key_base, resolve, reject);
                }
            });
        });
    });
}

function _createReduceChunks(job_entry, tasks, key_map, bucket, key_base, resolve, reject) {
    var shasum = crypto.createHash('sha1');
    var reduce_base_key = key_base + '/step_' + job_entry.current_step + '_bykey/';
    var chunks = [];
    var reducer_chunks = [];
    for (var reduce_key in key_map) {
        var key = reduce_base_key + shasum.update(reduce_key).digest('hex');
        chunks.push(_createReduceChunk(
            job_entry, key_map, key_map[reduce_key], bucket, key, key_base, reject
        ));
        reducer_chunks.push('AWS:' + bucket + ':' + key);
    }
    Promise.all(chunks).then(function () {
        utils.log(job_entry.id + ": All files created, cleaning up");
        for (var j=0; j<tasks.length; j++) {
            s3.deleteObject({
                Bucket: bucket, Key: key_base + '/' + tasks[j].id
            }).send();
            tasks[j].destroy();
        }
        resolve(reducer_chunks);
    });
}

function _createReduceChunk(job_entry, key_map, elements, bucket, key, key_base, reject) {
    new Promise(function (resolve_2) {
        utils.log(job_entry.id + ": Creating file " + key);
        s3.createMultipartUpload({
            Bucket: bucket,
            Key: key
        }, function (err, data) {
            if (err)
                return reject(
                    "Failed to allocate space for compaction of step " +
                    job_entry.current_step
                );

            var total = elements.length;
            var failed = false;
            var parts = []
            elements.forEach(function (element, index) { // element = [task_id, start, end]
                s3.uploadPartCopy({
                    Bucket: bucket, key: key, PartNumber: index,
                    UploadId: data.UploadId,
                    CopySource: bucket + '/' + key_base + '/' + element[0],
                    CopySourceRange: element[1] + '-' + element[2]
                }, function (err, copy_data) {
                    if (err) {
                        failed = true;
                        utils.log(job_entry.id + ": Failed to copy part", utils.ll.ERROR);
                    } else
                        parts[index] = copy_data.CopyPartResult.ETag;
                    total -= 1;
                    if (total == 0) { // all requests are done, cleanup
                        if (failed) {
                            s3.abortMultipartUpload({
                                Bucket: bucket, Key: key, UploadId: data.UploadId
                            }).send();
                            return reject("Failed to compact task at step " + job_entry.current_step);
                        }
                        var part_data = [];
                        for (var i=0; i<parts.length; i++) {
                            part_data[i] = {
                                ETag: parts[i],
                                PartNumber: i
                            }
                        }
                        s3.completeMultipartUpload({
                            Bucket: bucket, Key: key, UploadId: data.UploadId,
                            MultipartUpload: part_data
                        }, function(err, data) { resolve_2() });
                    }
                });
            });
        });
    });
}

module.exports.finish = function(job) {
    utils.log(job.id + ": Finishing the job (Cleanup, output, ...)");
    var output = job.output_dir.split(":");
    if (output[0] == 'AWS') {
        var final_output = job.id + '/_temp/step_' + job.current_step + '_final';
        s3.listObjects({
            Bucket: output[1],
            Prefix: job.id + '/_temp'
        }, function (err, data) {
            if (err) {
                utils.log(job.id + ": Failed to cleanup AWS (listObjects)", utils.ll.ERROR);
                // TODO handle error
            }
            del_params = {
                Bucket: output[1],
                Delete: {
                    Objects: []
                }
            };
            data.Contents.forEach(function (content) {
                    del_params.Delete.Objects.push({Key: content});
            });
            s3.copyObject({
                Bucket: output[1],
                Key: job.id + '_output',
                CopySource: output[1] + '/' + final_output
            }, function (err, data) {
                if (!err) {
                    s3.deleteObjects(del_params).send();
                } else {
                    utils.log(job.id + ": Failed to move final output (AWS)", utils.ll.ERROR);
                    job.error = "Failed to cleanup on job completion.";
                }
                job.completed = true;
                job.save();
            });
        });
    } else {
        // TODO implement local files
    }
}

/**
 * Populate the response with the task
 * according to the instance and step and job.
 * @param  {object} client DB entry for this client
 * @return {object} The task information required by the client.
 */
module.exports.schedule = function(client, storage, res, current_resp, prev_jobs) {
    if (current_resp === undefined)
        current_resp = {task: null};
    var search_params = {
        where : {
            completed : false,
            taken :0,
        },
        order: [Sequelize.fn('RANDOM')]
    }

    if (prev_jobs === undefined)
        prev_jobs = client.prev_jobs;

    // print some stats about the tasks table and job instances
    storage.Task.findAll().then(function (tasks) {
        if (tasks.length == 0) {
            utils.log("Task Stats: no tasks found");
        } else {
            var taken = 0, completed = 0;
            for (var i=0; i<tasks.length; i++) {
                if (tasks[i].taken > 0) taken++;
                if (tasks[i].completed) completed++;
            }
            utils.log("-- Task Stats: " + tasks.length + " tasks where " + taken +
                " are taken and " + completed + " are completed");
        }
        for (job_step in  instanceInfo) {
            var job_info = job_step.split("_");
            utils.log("-- Ins. Stats: job " + job_info[0] + "(" + job_info[1] + ")" +
                " has " + instanceInfo[job_step].length + " instances available");
        }
    });
    // end

    if (prev_jobs.length == 0) {
        utils.log("Searching for task (no prev jobs)");
        var available_job_step = null;
        var instance_id_ = null;
        for (job_step in  instanceInfo) {
            if (instanceInfo[job_step].length > 0) {
                available_job_step = job_step;
                instance_id_ = instanceInfo[job_step].shift();
                break;
            }
        }
        if (available_job_step == null)
            return res.json(current_resp);
        var job_info = available_job_step.split("_");
        search_params.where.instance = instance_id_;
        search_params.where.job_id = job_info[0];
        search_params.where.step = job_info[1];
        storage.Task.findOne(search_params).then(function (task) {
            if (!task) {
                utils.log("Couldn't find a task for client, params: " +
                    JSON.stringify(search_params));
                return res.json(current_resp);
            }
            utils.log("Found task for client with id: "+ task.id);
            var key = task.job_id.concat('_',task.step);
            task.taken = 1;
            task.attempts = task.attempts + 1;
            task.save();
            //send the bucket name and object key
            var file_split = task.input_file.split(':');
            var end_index = parseInt(task.input_offset) + parseInt(task.input_size);

            // update client prev_jobs
            if (client.prev_jobs.length > 4)
                client.prev_jobs.shift();
            client.prev_jobs = client.prev_jobs.push(task.job_id + ":" + task.step);
            client.task_id = task.id;
            client.save();
            
            current_resp.task = {
                task_id:task.id,
                step:task.step,
                instance_id:task.instance,
                bucket_name:file_split[1],
                object_key:file_split[2],
                start_index: parseInt(task.input_offset),
                end_index : end_index,
                job_id : task.job_id,
                access_key : commons.getConfig.credentials.accessKeyId,
                secret_key : commons.getConfig.credentials.secretAccessKey,
                aws_region : commons.getConfig.region
            };
            res.json(current_resp);
        }).catch(function() {
            utils.log("Failed to search for tasks, params: " +
                    JSON.stringify(search_params));
        });
    } else {
        utils.log("Searching for task (prev jobs: " + prev_jobs + ")");
        var job_info = prev_jobs.pop().split(":");
        var key = job_info[0].concat('_',job_info[1]);
        var instance_id_ = instanceInfo[key].shift();

        if(instance_id_ == undefined)
            return module.exports.schedule(client, storage, res, current_resp, prev_jobs);
        search_params.where.instance = instance_id_;
        search_params.where.job_id = job_info[0];
        search_params.where.step = job_info[1];
        storage.Task.findOne(search_params).then(function (task) {
            if (!task)
                return module.exports.schedule(client, storage, res, current_resp, prev_jobs);
            task.taken = 1;
            task.attempts = task.attempts + 1;
            task.save();

            // update client with task id
            client.task_id = task.id;
            client.save();

            //send the bucket name and object key
            var file_split = task.input_file.split(':');
            var end_index = task.input_offset + task.input_size;
            current_resp.task = {
                task_id:task.id,
                step:task.step,
                instance_id:task.instance,
                bucket_name:file_split[1],
                object_key:file_split[2],
                start_index:task.input_offset,
                end_index : end_index,
                job_id : task.job_id,
                access_key : commons.getConfig.credentials.accessKeyId,
                secret_key : commons.getConfig.credentials.secretAccessKey,
                aws_region : commons.getConfig.region
            };
            res.json(current_resp);
        });
    }
};
