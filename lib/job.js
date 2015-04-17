var mv = require('mv');
var path = require('path');
var Sequelize = require('sequelize');

var commons = require('../lib/commons.js');

/**
 * A Map which stores the number of instances for
 * each step
 */
var instanceInfo = {};

module.exports.instanceInfo = instanceInfo;

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
        mv(file_path, path.normalize(__dirname+'/../jobs/'+new_job.id+'.job'),
            function (err) {
                // TODO handle failed mv
            }
        );
        module.exports.addTasks(info, new_job, storage, conf.get('default_split_size'));
    });
};

/**
 * Add all tasks for the new job
 * @param {object} info     Job object loaded from the .js file
 * @param {object} db_entry DB entry representing this job
 * @param {object} storage  pointer to the DB ORM
 */
module.exports.addTasks = function(info, db_entry, storage, split_size) {
    var input = db_entry.input_file.split(":");
    // Check to see if the input exists on AWS
    if (input[0] == 'AWS') {
        var s3 = commons.getS3();
        s3.headObject({
            Bucket: input[1],
            Key: input[2]
        }, function (err, data) {
            if (err) {
                db_entry.error = err.code;
                db_entry.paused = true;
                db_entry.save();
            } else {
                _addTasksWithLen(
                    info, db_entry, storage, data.ContentLength, split_size);
            }
        });
    } else {
        // TODO add local storage support
    }
};

/**
 * Add all the necessary tasks for a job
 * @param {object} info       Job object loaded from the .js file
 * @param {object} db_entry   DB entry representing this job
 * @param {object} storage    pointer to the DB ORM
 * @param {int}    len        The length of the input file
 * @param {int}    split_size The split size of the input file
 */
function _addTasksWithLen(info, db_entry, storage, len, split_size) {
    var chain = new Sequelize.Utils.QueryChainer();
    var task_count = Math.ceil(len / split_size);
    var step0 = info.chain[0]();
    var instances = step0.instances;
    var sort = info.chain.length > 1 && (info.chain[1]()).is_reduce;
    //populate the instanceinfo
    var no_of_instances = [];
    for(var n = 0; n < instances ; n++)
        no_of_instances.push((n % instances) + 1);
    instanceInfo[db_entry.id.concat('_',db_entry.current_step)] = no_of_instances;

    // TODO change the insert to be a single insert instead (or batched at least)
    for (var i = 0; i<task_count; i++) {
        var params = {
            failed: 0, attempts: 0, replicates: 1, taken: 0, step: db_entry.current_step,
            completed: false,
            input_file: db_entry.input_file,
            input_offset: i*split_size,
            input_size: split_size,
            instance: instances == -1 ? null : ((i % instances) + 1),
            sort: sort,
            job_id:db_entry.id
        };
        chain.add(storage.Task.create(params));
    }
    chain.run().error(function() {
        db_entry.paused = true;
        db_entry.error = "Wasn't able to create all tasks for step "+(db_entry.current_step+1);
        db_entry.save();
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
        },
        order: [Sequelize.fn('RANDOM')]
    }

    if (prev_jobs === undefined)
        prev_jobs = client.prev_jobs;
    
    if (prev_jobs.length == 0) {
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
            if (!task)
                return res.json(current_resp);
            var key = task.job_id.concat('_',task.step);
            task.taken = 1;
            task.client_id = client.id;
            task.save();
            //send the bucket name and object key
            var file_split = task.input_file.split(':');
            var end_index = parseInt(task.input_offset) + parseInt(task.input_size);

            // update client prev_jobs
            if (client.prev_jobs.length > 4)
                client.prev_jobs.shift();
            client.prev_jobs = client.prev_jobs.push(task.job_id + ":" + task.step);
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
                aws_region : commons.getConfig.credentials.region
            };
            res.json(current_resp);
        });
    } else {
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
            task.client_id = client.id;
            task.save();
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
                aws_region : commons.getConfig.credentials.region
            };
            res.json(current_resp);
        });
    }
};

