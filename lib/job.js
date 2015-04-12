var mv = require('mv');
var path = require('path');
var Sequelize = require('sequelize');

var commons = require('../lib/commons.js');

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
    // TODO change the insert to be a single insert instead (or batched at least)
    for (var i = 0; i<task_count; i++) {
        var params = {
            failed: 0, attempts: 0, replicates: 1, taken: 0, step: db_entry.current_step,
            input_file: db_entry.input_file,
            input_offset: i*split_size,
            input_size: split_size,
            instance: instances == -1 ? null : ((i % instances) + 1),
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

/**
 * Return a new task for this client.
 * @param  {object} client DB entry for this client
 * @return {object} The task information required by the client.
 */
module.exports.schedule = function(client) {
    // TODO schedule the job
};
