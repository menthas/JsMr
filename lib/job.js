var mv = require('mv');
var path = require('path');
var Sequelize = require('sequelize');
var Promise = require('bluebird');
var crypto = require('crypto');

var commons = require('../lib/commons.js');
var s3 = commons.getS3();

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
        error: null,
    }).then(function (new_job) {
        mv(file_path, path.normalize(__dirname+'/../jobs/'+new_job.id+'.job'),
            function (err) {
                // TODO handle failed mv
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
    var sort = info.chain.length > db_entry.current_step + 1 &&
        (info.chain[db_entry.current_step + 1]()).is_reduce;
    // TODO change the insert to be a single insert instead (or batched at least)
    for (var i = 0; i<task_count; i++) {
        var task_instance = instance === undefined ? 
            (instances == -1 ? null : ((i % instances) + 1)) : instance;
        var params = {
            failed: 0, attempts: 0, replicates: 1, taken: 0, step: db_entry.current_step,
            input_file: input_source,
            input_offset: i*split_size,
            input_size: split_size,
            instance: task_instance,
            sort: sort
        };
        chain.add(storage.Task.create(params));
    }
    chain.run().error(function() {
        db_entry.paused = true;
        db_entry.error = "Wasn't able to create all tasks for step "+(db_entry.current_step+1);
        db_entry.save();
    });
}

module.exports.compactStep = function(job_entry, conf) {
    return storage.Task.findAll({
        where: {
            job_id: job_entry.id,
            step: job_entry.current_step,
            completed: true
        }
    }).then(function (tasks) {
        if (!tasks)
            return;
        var need_sort = tasks[0].sort;
        if (need_sort)
            return _shuffleAndCompact(job_entry, tasks, conf.get("server_split_size"));
        else
            return _compact(job_entry, tasks);
    });
};

function _compact(job_entry, tasts) {
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
                    Bucket: bucket, Key: key, PartNumber: index,
                    UploadId: data.UploadId,
                    CopySource: bucket + '/' + key_base + '/' + task.id
                }, function (err, copy_data) {
                    if (err)
                        failed = true;
                    else
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
                                    PartNumber: i
                                }
                            }
                            s3.completeMultipartUpload({
                                Bucket: bucket, Key: key, UploadId: data.UploadId,
                                MultipartUpload: part_data
                            });
                        }
                        for (var j=0; j<tasks.length; j++) {
                            s3.deleteObject({
                                Bucket: bucket, Key: key_base + '/' + tasks[j].id
                            }).send();
                            tasks[j].destroy();
                        }
                    }
                    resolve(['AWS:' + bucket + ':' + key]);
                });
            });
        });
    });
}

function _shuffleAndCompact(job_entry, tasks, download_size) {
    return new Promise(function (resolve, reject) {
        var output = job_entry.output_dir.split(":")
        if (output[0] != 'AWS')
            return reject("None AWS outputs are not supported.");

        var bucket = output[1];
        var key_base = job_entry.id + '/_temp';
        var total_tasks = tasks.length;
        tasks.forEach(function (task) {
            var key_map = {};
            s3.getObject({Bucket: bucket, Key: key_base + '/' + task.id}).
            on('httpData', function (chunk) {
                var lines = chunk.split("\n");
                var offset = 0;
                var old_key = null;
                var key = null;
                for (var i=0; i<lines.length; i++) {
                    key = lines[i].split("\t")[0];
                    if (key !== old_key) {
                        if (old_key != null) // add bound for the old key
                            key_map[old_key][key_map.length-1].push(offset);
                        // add the new key
                        if (!(key in key_map))
                            key_map[key] = [];
                        key_map[key].push([task.id, offset]);
                        old_key = key;
                    }
                    offset += len(lines[i]) + 1;
                }
                if (key != null) // add bound for the last key
                    key_map[key][key_map.length-1].push(offset);
            }).
            on('httpDone', function () {
                total_tasks -= 1;
                if (total_tasks == 0) { // once all tasks are done
                    _createReduceChunks(tasks, key_map, bucket, key_base, resolve, reject);
                }
            });
        });
    });
}

function _createReduceChunks(tasks, key_map, bucket, key_base, resolve, reject) {
    var shasum = crypto.createHash('sha1');
    var reduce_base_key = key_base + '/step_' + job_entry.current_step + '_bykey/';
    var chunks = [];
    var reducer_chunks = [];
    for (var reduce_key in key_map) {
        var key = reduce_base_key + shasum.update(reduce_key).digest('hex');
        chunks.push(_createReduceChunk(
            key_map, key_map[reduce_key], bucket, key, key_base, reject
        ));
        reducer_chunks.push('AWS:' + bucket + ':' + key);
    }key
    Promise.all(chunks).then(function () {
        for (var j=0; j<tasks.length; j++) {
            s3.deleteObject({
                Bucket: bucket, Key: key_base + '/' + tasks[j].id
            }).send();
            tasks[j].destroy();
        }
        resolve(reducer_chunks);
    });
}

function _createReduceChunk(key_map, elements, bucket, key, key_base, reject) {
    new Promise(function (resolve_2) {
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
                    if (err)
                        failed = true;
                    else
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
    var output = job.output_dir.split(":");
    if (output[0] == 'AWS') {
        var final_output = job.id + '/_temp/step_' + job.current_step + '_final';
        s3.listObjects({
            Bucket: output[1],
            Prefix: job_id + '/_temp'
        }, function (err, data) {
            if (err) {
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
                Key: job_id + '_output',
                CopySource: output[1] + '/' + final_output
            }, function (err, data) {
                if (!err) {
                    s3.deleteObjects(del_params).send();
                } else {
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
 * Return a new task for this client.
 * @param  {object} client DB entry for this client
 * @return {object} The task information required by the client.
 */
module.exports.schedule = function(client) {
    // TODO schedule the job
};
