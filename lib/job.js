var mv = require('mv');
var path = require('path');
var os = require('os');
var Sequelize = require('sequelize');
var Promise = require('bluebird');
var crypto = require('crypto');
var fs = Promise.promisifyAll(require('fs'));

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
        var full_source, key = null;
        if (typeof input_source == 'object') { // reduce job (no key on file).
            full_source = input_source.file;
            key = input_source.key;
        } else
            full_source = input_source;

        var input = full_source.split(":");
        // Check to see if the input exists on AWS
        if (input[0] == 'AWS') {
            commons.S3Head(s3, input[1], input[2]).then(function (data) {
                instance = undefined;
                if (is_reduce) {
                    instance = (instances == -1 ? index : ((index % instances) + 1));
                }
                _addTasksWithLen(
                        info, db_entry, full_source, storage,
                        data.ContentLength, split_size, instance, key);
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
function _addTasksWithLen(info, db_entry, input_source, storage, len, split_size, instance, key) {
    var chain = new Sequelize.Utils.QueryChainer();
    var task_count = Math.ceil(len / split_size);
    var cur_step_func = info.chain[db_entry.current_step]();
    var instances = cur_step_func.instances;

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
            key: key,
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

/**
 * Add a single cleanup task for a step of a job. This task will see all the
 * states.
 * @param {object} job     job DB entry
 * @param {object} storage storage backend
 */
module.exports.addCleanupTask = function(job, storage) {
    var input_file = job.input_file.split(":");
    storage.Task.create({
        failed: 0, attempts: 0, replicates: 1, taken: 0, step: job.current_step,
        completed: false,
        input_file: 'CLEANUP:' + input_file[1],
        instance: null,
        job_id: job.id
    }).then(function () {
        utils.log(job.id + ": Cleanup task created.");
    }).catch(function () {
        utils.log(job.id + ": Failed to create cleanup task.");
    })
}

/**
 * Entry point for compacting a step. This function will decide how to compact
 * the job.
 * @param  {object} job_entry DB entry
 * @param  {object} job_info  Job info and code
 * @param  {object} conf      configuration object
 * @param  {object} storage   storage access object
 * @return {Promise}          This resolves if compaction finishes successfully
 *     and provides a list of new files for the next/final step.
 *     Or is rejected if the compactions files providing an error message.
 */
module.exports.compactStep = function(job_entry, job_info, conf, storage) {
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
        var need_sort = job_info.chain.length > job_entry.current_step + 1 &&
            (job_info.chain[job_entry.current_step + 1]()).is_reduce;
        if (need_sort)
            return _shuffleAndCompact(job_entry, tasks, conf.get("shuffle_mem_limit"));
        else
            return _compact(job_entry, tasks);
    });
};

/**
 * Compact a job by concating all the task outputs into a single file.
 * @param  {object} job_entry DB entry
 * @param  {list}   tasks     list of tasks for the step
 * @return {Promise}          This resolves if compaction finishes successfully
 *     and provides a list of new files for the next/final step.
 *     Or is rejected if the compactions files providing an error message.
 */
function _compact(job_entry, tasks) {
    utils.log(job_entry.id + ": Concat compaction (no reducer)");
    return new Promise(function (resolve, reject) {
        var output = job_entry.output_dir.split(":")
        if (output[0] != 'AWS')
            return reject("None AWS outputs are not supported.");

        var bucket = output[1];
        var key_base = job_entry.id + '/_temp';
        var key = key_base + '/step_' + job_entry.current_step + '_final';

        _getAllSplitSizes(tasks, bucket).then(function (sizes) {
            var under_5 = 0, total = 0;
            for (task_id in sizes) {
                if (sizes[task_id] == -1) continue; // no output file
                if (sizes[task_id] <= 5242880) under_5++;
                total++;
            }
            utils.log(job_entry.id + ": Output sizes calculated, " + under_5 + " out of " +
                total + " are under 5MB");

            s3.createMultipartUpload({
                Bucket: bucket,
                Key: key
            }, function (err, data) {
                if (err)
                    return reject(
                        "Failed to allocate space for compaction of step " +
                        job_entry.current_step
                    );

                var params = {
                    job: job_entry,
                    total: tasks.length, failed: false, parts: [],
                    sizes: sizes,
                    bucket: bucket, key_base: key_base, key: key, upload_id: data.UploadId,
                    buffer: "", buffer_size: 0
                }
                tasks.forEach(function (task, index) {
                    _handleTaskOutput(tasks, task, index, params, resolve, reject);
                });
            });
        }).catch(function (err) { reject(err); });
    });
}

/**
 * Returns output file sizes for a given list of tasks
 * @param  {list}    tasks
 * @param  {object}  bucket AWS bucket
 * @return {Promise}
 *     Resolve: {id: size, id: size, ...}
 *     Reject: string, error message
 */
function _getAllSplitSizes(tasks, bucket) {
    var total = tasks.length;
    var sizes = {};
    return new Promise(function (resolve, reject) {
        tasks.forEach(function (task) {
            var key = task.job_id + '/_temp/' + task.id;
            commons.S3Head(s3, bucket, key).then(function (data) {
                sizes[task.id] = data.ContentLength;
                total--;
                if (total == 0)
                    resolve(sizes);
            }).catch(function (err) {
                if (err.statusCode !== 404)
                    return reject(err);
                sizes[task.id] = -1;
                total--;
                if (total == 0)
                    resolve(sizes);
            });
        });
    });
}

/**
 * Handles the output of a single task. which means:
 *   1. if the task is bigger than 5MB, copy it directly on AWS
 *   2. if the task is smaller than 5MB, download it and add it to the buffer.
 * This will make sure all data is tranfered if this is the last task to finish
 * @param  {list}     tasks   list of all tasks in this compaction job
 * @param  {object}   task    a member of the list above that we currently see
 * @param  {integer}  index   a unique integer processing index
 * @param  {object}   params  an object of parameters for this compaction
 * @param  {function} resolve will be called on success
 * @param  {function} reject  will be called on failure
 * @return {void}
 */
function _handleTaskOutput(tasks, task, index, params, resolve, reject) {
    // task with no output
    if (params.sizes[task.id] == -1) {
        params.total--;
        if (params.buffer_size > 5242880 || params.total == 0) {
            debugger;
            utils.log(params.job.id + ": Buffer full or all seen all tasks, flushing");
            s3.uploadPart({
                Bucket: params.bucket, Key: params.key, PartNumber: index + 1,
                UploadId: params.upload_id, Body: params.buffer
            }, function (err, data) {
                if (err) {
                    params.failed = true;
                } else {
                    params.parts[index] = data.ETag;
                    params.buffer = "";
                    params.buffer_size = 0;
                }
                if (params.total == 0) {  // all requests are done, cleanup
                    _completeCompactUpload(tasks, resolve, reject, params);
                }
            });
        }
    } else if (params.sizes[task.id] <= 5242880) {
        // AWS S3 has a min part size of 5MB so if the chunk is smaller we need to
        // buffer it.
        s3.getObject({Bucket: params.bucket, Key: params.key_base + '/' + task.id}).
        on('httpData', function (chunk) {
            params.buffer += chunk;
            params.buffer_size += chunk.length;
        }).
        on('httpDone', function () {
            params.total--;
            if (params.buffer_size > 5242880 || params.total == 0) {
                utils.log(params.job.id + ": Buffer full or all seen all tasks, flushing");
                s3.uploadPart({
                    Bucket: params.bucket, Key: params.key, PartNumber: index + 1,
                    UploadId: params.upload_id, Body: params.buffer
                }, function (err, data) {
                    if (err) {
                        params.failed = true;
                    } else {
                        params.parts[index] = data.ETag;
                        params.buffer = "";
                        params.buffer_size = 0;
                    }
                    if (params.total == 0) {  // all requests are done, cleanup
                        _completeCompactUpload(tasks, resolve, reject, params);
                    }
                });
            }
        }).send();
    } else { // part bigger than 5MB, upload directly
        s3.uploadPartCopy({
            Bucket: params.bucket, Key: params.key, PartNumber: index + 1,
            UploadId: data.UploadId,
            CopySource: params.bucket + '/' + params.key_base + '/' + task.id
        }, function (err, copy_data) {
            if (err) {
                utils.log(params.job.id + ": Failed to copy AWS part", utils.ll.ERROR);
                params.failed = true;
            } else
                params.parts[index] = copy_data.CopyPartResult.ETag;
            params.total --;
            if (params.total == 0) { // all requests are done, cleanup
                _completeCompactUpload(tasks, resolve, reject, params);
            }
            utils.log(params.job.id + ": Concat Complete !");
        });
    }
}

/**
 * After all chunks of a compaction job are handled this function will close
 * the compaction job and cleanup.
 * @param  {list}     tasks   list of all tasks in compaction
 * @param  {function} resolve will be called on success
 * @param  {function} reject  will be called on failure
 * @param  {object}   params  object of compaction parameters
 * @return {void}
 */
function _completeCompactUpload(tasks, resolve, reject, params) {
    if (params.failed) {
        s3.abortMultipartUpload({
            Bucket: params.bucket, Key: params.key, UploadId: params.upload_id
        }).send();
        return reject("Failed to compact task at step " + params.job.current_step +
            " (Part upload failed)");
    } else {
        var part_data = [];
        for (var i=0; i<params.parts.length; i++) {
            if (!params.parts[i])
                continue;
            part_data[i] = {
                ETag: params.parts[i],
                PartNumber: i + 1
            }
        }
        s3.completeMultipartUpload({
            Bucket: params.bucket, Key: params.key, UploadId: params.upload_id,
            MultipartUpload: { Parts: part_data }
        }, function (err, data) {
            if (err) {
                reject("Failed to complete compaction (multipart upload failed)");
            } else {
                utils.log(params.job.id + ": Moved all chunks to the new file, " +
                    "Cleaning up");
                for (var j=0; j<tasks.length; j++) {
                    if (params.sizes[tasks[j].id] != -1) // if it had an output, delete it
                        s3.deleteObject({
                            Bucket: params.bucket, Key: params.key_base + '/' + tasks[j].id
                        }).send();
                    tasks[j].destroy(); // remove from tasks table
                    resolve(['AWS:' + params.bucket + ':' + params.key]);
                }
            }
        });
    }
}

/**
 * Handles compaction when the next step is a reducer. shuffles the outputs and
 * creates a "per-key" file on AWS
 *
 * @implementation
 *     This code will read the data from AWS into memory (sorted by key) until
 *     the `mem_limit` is reached. Then the contents are flushed to disk or
 *     uploaded to AWS (both memory and disk) if it has reached the AWS limit.
 * 
 * @param  {object}  job_entry DB entry
 * @param  {list}    tasks     list of tasks in this compaction job
 * @param  {integer} mem_limit maximum amount of memory to use for the buffer
 * @return {Promise}           This resolves if compaction finishes successfully
 *     and provides a list of new files for the next/final step.
 *     Or is rejected if the compactions files providing an error message.
 */
function _shuffleAndCompact(job_entry, tasks, mem_limit) {
    utils.log(job_entry.id + ": Shuffle Compaction (reducer next)");
    return new Promise(function (resolve, reject) {
        var output = job_entry.output_dir.split(":")
        if (output[0] != 'AWS')
            return reject("None AWS outputs are not supported.");

        var params = {
            job: job_entry,
            bucket: output[1],
            key_base: job_entry.id + '/_temp',
            total_tasks: tasks.length,
            task_counter: tasks.length,
            total_size: 0,
            multipart_uploads: {},
            tasks: tasks
        }
        var key_map = {};
        var key_map_sizes = {};
        var total_size = 0;
        var num_keys = 0;
        tasks.forEach(function (task) {
            var task_output = "";
            s3.getObject({Bucket: params.bucket, Key: params.key_base + '/' + task.id}).
            on('httpData', function (chunk) {
                task_output += chunk;
            }).
            on('httpDone', function () {
                // Read the object line by line and put into the right bucket
                // based on key.
                var lines = task_output.split("\n");
                var line = null;
                for (var i=0; i<lines.length; i++) {
                    if (!lines[i]) // border case of the last line
                        continue;
                    line = lines[i].split(":");
                    if (key_map[line[0]] === undefined) {
                        key_map[line[0]] = "";
                        key_map_sizes[line[0]] = 0;
                        num_keys++;
                    }
                    key_map[line[0]] += line[1] + "\n";
                    key_map_sizes[line[0]] += line[1].length + 1;
                    params.total_size += line[1].length + 1;
                }
                lines = undefined; task_output = undefined;
                params.task_counter--;
                utils.log(job_entry.id + ": Parse complete. " +
                          params.task_counter + "/" + params.total_tasks + " remaining");
                var last_task = params.task_counter == 0;
                if (last_task || params.total_size > mem_limit) { // flush and finalize
                    _flushShuffleBuffer(key_map, key_map_sizes, params, last_task).
                    then(function () {
                        if (last_task)
                            _uploadShuffleKeys(params, resolve, reject);
                    }).catch(function (err) { reject(err); });
                }
            }).
            on('error', function (err) {
                if (err.statusCode != 404) // some unhandled error happened
                    return reject(JSON.stringify(err));
                params.task_counter--;
                utils.log(job_entry.id + ": No output found (not ommited)" +
                          params.task_counter + "/" + params.total_tasks + " remaining");
                var last_task = params.task_counter == 0;
                // we still need to finish the job if this was the last task
                if (last_task || params.total_size > mem_limit) {
                    _flushShuffleBuffer(key_map, key_map_sizes, params, last_task).
                    then(function () {
                        if (last_task)
                            _uploadShuffleKeys(params, resolve, reject);
                    }).catch(function (err2) { reject(err2); });
                }
            }).send();
        });
    });
}

/**
 * Flush the compaction buffer to disk or flush everything to AWS if >5MB
 * @param  {object}  key_map       object of all data seen sorted by key
 * @param  {object}  key_map_sizes size of each `key`
 * @param  {object}  params        object of compaction parameters
 * @param  {boolean} force         flag to force an upload to AWS
 * @return {Promise}               Resolved on success and rejected on failure
 */
function _flushShuffleBuffer(key_map, key_map_sizes, params, force) {
    return new Promise(function (resolve, reject) {
        var tmp_dir = os.tmpdir();
        var files = [];
        // Set of all writes and uploads
        Object.keys(key_map).forEach(function (key) {
            var hash = crypto.createHash('sha1').update(key).digest('hex');
            var file_name = tmp_dir + '/jsmr_' + hash;
            var file_size = 0;
            try {
                file_size = fs.statSync(file_name).size;
            } catch (err) {}
            if (force || file_size + key_map_sizes[key] > 5242880) {
                if (file_size > 0)
                    files.push(fs.readFileAsync(file_name).then(function (content) {
                        return _uploadShufflePart(key, hash, content + key_map[key], params, file_name);
                    }));
                else
                    files.push(_uploadShufflePart(key, hash, key_map[key], params));
            } else {
                files.push(fs.writeFileAsync(file_name, key_map[key], "utf-8"));
            }
        });

        // wait for all of them to finish
        Promise.all(files).then(function () {
            utils.log(params.job.id + ": Flushed shuffle buffer to disk at " +
                utils.size_string(params.total_size));
            for (key in key_map) {
                key_map[key] = "";
                key_map_sizes[key] = 0;
            }
            params.total_size = 0;
            resolve();
        }).catch(function () {
            // Abort all parts
            for (key in params.multipart_uploads) {
                var aws_key = params.key_base + '/step_' + params.job.current_step + '_bykey/' +
                    params.multipart_uploads[key].hash;
                s3.abortMultipartUpload({
                    Bucket: params.bucket, Key: aws_key, UploadId: params.multipart_uploads[key].id
                }).send();
            }
            reject("Failed to flush shuffle files to disk (full ?)");
        });
    });
}

/**
 * Upload a single part of a key's data to AWS and remove any flushed data
 * from disk.
 * @param  {string} key        the output key we're aggregating on
 * @param  {string} hash       the hash of the given key
 * @param  {string} content    content of this part
 * @param  {object} params     object of compaction parameters
 * @param  {string} file_name  the filename of the flushed data on disk
 * @return {Promise}           Resolved on success, Rejected on failure (with error)
 */
function _uploadShufflePart(key, hash, content, params, file_name) {
    return new Promise(function (resolve, reject) {
        aws_key = params.key_base + '/step_' + params.job.current_step + '_bykey/' + hash;
        if (params.multipart_uploads[key] == undefined) {
            s3.createMultipartUpload({
                Bucket: params.bucket,
                Key: aws_key, Metadata: {key: key}
            }, function (err, data) {
                if (err)
                    return reject("Failed to allocate space for shuffle of step " +
                        params.job.current_step);
                params.multipart_uploads[key] = {
                    id: data.UploadId,
                    hash: hash,
                    parts: []
                }
                s3.uploadPart({
                    Bucket: params.bucket, Key: data.Key,
                    PartNumber: 1,
                    UploadId: data.UploadId, Body: content
                }, function (err2, data2) {
                    if (err2)
                        return reject(JSON.stringify(err2));
                    params.multipart_uploads[key].parts.push(data2.ETag);
                    file_name && fs.unlinkSync(file_name);
                    resolve();
                });
            });
        } else {
            s3.uploadPart({
                Bucket: params.bucket, Key: aws_key,
                PartNumber: params.multipart_uploads[key].parts.length + 1,
                UploadId: data.UploadId, Body: content
            }, function (err, data) {
                if (err)
                    return reject(JSON.stringify(err));
                params.multipart_uploads[key].parts.push(data.ETag);
                file_name && fs.unlinkSync(file_name);
                resolve();
            });
        }
    });
}

/**
 * Complete a compaction shuffle job. close any open connections to AWS
 * @param  {object}   params  parameters of this compaction job
 * @param  {function} resolve called with new files on success
 * @param  {function} reject  called otherwise with error
 * @return {void}
 */
function _uploadShuffleKeys(params, resolve, reject) {
    utils.log(params.job.id + ": All reduce chunks were handled, completing the uploads.");
    var total = 0;
    for (key in params.multipart_uploads)
        total++;

    var reduced_files = [];
    for (key in params.multipart_uploads) {
        aws_key = params.key_base + '/step_' + params.job.current_step + '_bykey/' +
            params.multipart_uploads[key].hash;
        var part_data = [];
        for (var i=0; i<params.multipart_uploads[key].parts.length; i++) {
            part_data[i] = {
                ETag: params.multipart_uploads[key].parts[i],
                PartNumber: i + 1
            }
        }
        reduced_files.push({
            file:'AWS:' + params.bucket + ':' + aws_key,
            key: key
        });
        s3.completeMultipartUpload({
            Bucket: params.bucket, Key: aws_key, UploadId: params.multipart_uploads[key].id,
            MultipartUpload: { Parts: part_data }
        }, function(err, data) {
            if (err)
                reject(JSON.stringify(err));
            total--;
            if (total == 0) {
                utils.log(params.job.id + ": All shuffled files created, cleaning up");
                for (var j=0; j<params.tasks.length; j++) {
                    s3.deleteObject({
                        Bucket: params.bucket, Key: params.key_base + '/' + params.tasks[j].id
                    }).send();
                    params.tasks[j].destroy();
                }
                resolve(reduced_files);
            }
        });
    }
}

/**
 * Finish a job. cleanup the temp data and create final output.
 * @param  {object} job DB entry
 * @return {void}
 */
module.exports.finish = function(job) {
    return new Promise(function (resolve, reject) {
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
                        del_params.Delete.Objects.push({Key: content.Key});
                });
                s3.copyObject({
                    Bucket: output[1],
                    Key: job.id + '_output',
                    CopySource: output[1] + '/' + final_output
                }, function (err, data) {
                    if (!err) {
                        s3.deleteObjects(del_params).send();
                        utils.log(job.id + ": All done! find job output at " + final_output);
                    } else {
                        utils.log(job.id + ": Failed to move final output (AWS)", utils.ll.ERROR);
                        job.error = "Failed to cleanup on job completion.";
                    }
                    job.completed = true;
                    job.save();
                    resolve();
                });
            });
        } else {
            // TODO implement local files
        }
    });
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
            instance: {
                $or: [
                    null,
                    null
                ]
            }
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

    // no prev jobs, find any job and serve
    if (prev_jobs.length == 0) {
        utils.log("Scheduler: Searching for task (no prev jobs)");
        var available_job_step = null;
        var instance_id_ = null;
        for (job_step in  instanceInfo) {
            if (instanceInfo[job_step].length > 0) {
                available_job_step = job_step;
                instance_id_ = instanceInfo[job_step].shift();
                break;
            }
        }
        if (available_job_step == null) {
            utils.log("Scheduler: no instances available");
            return res.json(current_resp);
        }
        var job_info = available_job_step.split("_");
        search_params.where.instance["$or"][0] = instance_id_;
        search_params.where.job_id = job_info[0];
        search_params.where.step = job_info[1];
        storage.Task.findOne(search_params).then(function (task) {
            if (!task) {
                utils.log("Scheduler: Couldn't find a task for client");
                // push back the instance
                instanceInfo[available_job_step].push(instance_id_);
                return res.json(current_resp);
            }
            utils.log("Scheduler: Found task for client with id: "+ task.id);
            var key = task.job_id.concat('_',task.step);
            task.taken = 1;
            task.attempts = task.attempts + 1;
            task.save();

            // update client prev_jobs
            if (client.prev_jobs.length > 4)
                client.prev_jobs.shift();
            client.prev_jobs = client.prev_jobs.push(task.job_id + ":" + task.step);
            client.task_id = task.id;
            client.save();

            var file_split = task.input_file.split(':');
            if (file_split[0] == 'CLEANUP') { // found a cleanup job
                // this shouldn't use an instance, push back the instance
                instanceInfo[available_job_step].push(instance_id_);
                current_resp.task = {
                    task_id:task.id,
                    step:task.step,
                    special: 'cleanup',
                    bucket_name: file_split[1],
                    job_id : task.job_id,
                    access_key : commons.getConfig.credentials.accessKeyId,
                    secret_key : commons.getConfig.credentials.secretAccessKey,
                    aws_region : commons.getConfig.region
                }
            } else { // found a normal job
                var end_index = parseInt(task.input_offset) + parseInt(task.input_size);
                current_resp.task = {
                    task_id: task.id,
                    step: task.step,
                    task_key: task.key,
                    instance_id: task.instance,
                    bucket_name: file_split[1],
                    object_key: file_split[2],
                    start_index: parseInt(task.input_offset),
                    end_index : end_index,
                    job_id : task.job_id,
                    access_key : commons.getConfig.credentials.accessKeyId,
                    secret_key : commons.getConfig.credentials.secretAccessKey,
                    aws_region : commons.getConfig.region
                };
            }
            res.json(current_resp);
        }).catch(function() {
            utils.log("Scheduler: Failed to search for tasks, params: " +
                    JSON.stringify(search_params));
        });
    } else {
        // Find a job based on the prev jobs the client finished
        utils.log("Scheduler: Searching for task (prev jobs: " + prev_jobs + ")");
        var job_info = prev_jobs.pop().split(":");
        var key = job_info[0].concat('_',job_info[1]);
        var instance_id_ = instanceInfo[key].shift();

        if(instance_id_ == undefined)
            return module.exports.schedule(client, storage, res, current_resp, prev_jobs);
        search_params.where.instance["$or"][0] = instance_id_;
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

            var file_split = task.input_file.split(':');
            if (file_split[0] == 'CLEANUP') {
                current_resp.task = {
                    task_id: task.id,
                    step: task.step,
                    special: 'cleanup',
                    bucket_name: file_split[1],
                    job_id : task.job_id,
                    access_key : commons.getConfig.credentials.accessKeyId,
                    secret_key : commons.getConfig.credentials.secretAccessKey,
                    aws_region : commons.getConfig.region
                }
            } else {
                var end_index = parseInt(task.input_offset) + parseInt(task.input_size);
                current_resp.task = {
                    task_id: task.id,
                    step: task.step,
                    task_key: task.key,
                    instance_id: task.instance,
                    bucket_name: file_split[1],
                    object_key: file_split[2],
                    start_index: task.input_offset,
                    end_index : end_index,
                    job_id : task.job_id,
                    access_key : commons.getConfig.credentials.accessKeyId,
                    secret_key : commons.getConfig.credentials.secretAccessKey,
                    aws_region : commons.getConfig.region
                };
            }
            res.json(current_resp);
        });
    }
};
