var mv = require('mv');
var path = require('path');

/**
 * Add a new job to the queue
 * @param {string} file_path temporary path of the job .js file
 * @param {object} info      the loaded job object
 * @param {object} db_params parameters that are required for the DB
 * @param {object} storage   pointer to the database ORM
 */
module.exports.add = function (file_path, info, db_params, storage) {
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
        input_file: db_input,
        output_dir: db_output,
        completed: false,
        paused: db_params.paused
    }).then(function (new_job) {
        mv(file_path, path.normalize(__dirname+'/../jobs/'+new_job.id+'.js'),
            function (err) {
                // TODO handle failed mv
            }
        );
        
        module.exports.addTasks(info, new_job, storage);
    });
};

/**
 * Add all tasks for the new job
 * @param {object} info     Job object loaded from the .js file
 * @param {object} db_entry DB entry representing this job
 * @param {object} storage  pointer to the DB ORM
 */
module.exports.addTasks = function(info, db_entry, storage) {
    // TODO schedule the job
};

/**
 * Return a new task for this client.
 * @param  {object} client DB entry for this client
 * @return {object} The task information required by the client.
 */
module.exports.schedule = function(client) {
    // TODO schedule the job
};
