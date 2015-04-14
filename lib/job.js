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
    // TODO change the insert to be a single insert instead (or batched at least)
    for (var i = 0; i<task_count; i++) {
        var params = {
            failed: 0, attempts: 0, replicates: 1, taken: 0, step: db_entry.current_step,
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
 * Return a new task for this client.
 * @param  {object} client DB entry for this client
 * @return {object} The task information required by the client.
 */
module.exports.schedule = function(client,storage,res) {
    // TODO schedule the job
    /*var chain = new Sequelize.Utils.QueryChainer();
    var prev_jobs = client.previous_job;
    for(i in prev_jobs) {
        chain.add(storage.Task.findOne({
            where:{
                taken: 0,
                job_id: prev_jobs[i].job_id,
                step:prev_jobs[i].step
            }
        }));
    }
    //If prev_jobs was empty or no task with same job and same step is available
    if(chain.length < 1){
       chain.add(storage.Task.findOne({
           where:{
               taken:0
           }
       }))
    }

    chain.run().success(function(){

    })

  */
        storage.Task.findOne({
            where: {
                taken: 0
            }
             //todo add condition for correct stage
        }).then(function (task) {
            if (!task)
                return;
            task.taken = 1;
            task.client_id = client.id;
            task.save();
            //send the bucket name and object key
            var file_split = task.input_file.split(':');
            var end_index = task.input_offset + task.input_size;
	    console.log
            res.json({
                registered: true,
                client_id: client.id,
                task: {
                    task_id:task.id,
                    step:task.step,
                    instance_id:task.instance,
                    bucket_name:file_split[1],
                    object_key:file_split[2],
                    start_index:task.input_offset,
                    end_index : end_index,
                    job_id : task.job_id,
                    access_key : commons.getConfig.credentials.accessKeyId,
                    secret_key : commons.getConfig.credentials.secretAccessKey
                }
            });
        }).catch(function (error) {
            console.log(error);
            res.json({
                registered: false
            });
        });

    //pick up a task from the task table
/*    var prev_jobs = client.previous_job;
    for(i in prev_jobs){
        var client_job_id = prev_jobs[i].job_id;
        storage.Task.findOne({
            where: Sequelize.and({
                taken: 0
            },{
                client_job_id:job_id
            }) //todo add condition for correct stage
        }).then(function (task) {
            if (!task)
                return;
            console.log("Found a task" + task.id);
            //update the task
            task.updateAttributes({
                taken:1,
                client_id:client.id
            }).success(function (){


                var temp = task.input_file.split(':');
                task.bucket_name = temp[1];
                task.object_key = temp[2];

                return task;

            })
        }).catch(function (error) {
            console.log('Could not find a task');
        });
    }
*/
};

