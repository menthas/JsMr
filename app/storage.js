var Sequelize = require('sequelize');

// setup the database instance
var sequelize = new Sequelize(
    'main', 'user', 'pass', {
        dialect: 'sqlite',
        storage: module.parent.exports.conf.get('sqlite_storage'),
        pool: {
            max: 5,
            min: 0,
            idle: 10000
        },
        define: {
            underscored: true,
            paranoid: false
        }
    }
);

/**
 * The Client Model (Table). Used to save active clients and their statistics
 */
var Client = sequelize.define('client', {
    id: {
        type: Sequelize.UUID,
        defaultValue: Sequelize.UUIDV4,
        primaryKey: true,
    },
    auth_token: Sequelize.STRING,
    busy: {
        type: Sequelize.BOOLEAN,
        allowNull: false,
        defaultValue: false
    },
    agent: Sequelize.STRING,
    tasks_done: Sequelize.INTEGER.UNSIGNED,
    tasks_failed: Sequelize.INTEGER.UNSIGNED,
    last_activity: Sequelize.DATE,
    prev_jobs: {
        type: Sequelize.STRING,
        get: function() {
            return JSON.parse(this.getDataValue('prev_jobs'));
        },
        set: function(val) {
            this.setDataValue(JSON.stringify(val));
        }
    }
});

/**
 * Job Module (Table), used to store information about jobs
 */
var Job = sequelize.define('job', {
    id: {
        type: Sequelize.UUID,
        defaultValue: Sequelize.UUIDV4,
        primaryKey: true,
    },
    name: Sequelize.STRING,
    input_file: Sequelize.STRING,
    output_dir: Sequelize.STRING,
    completed: Sequelize.BOOLEAN,
    paused: Sequelize.BOOLEAN,
});

/**
 * Task Module (Table), used to store Job tasks and their status
 */
var Task = sequelize.define('task', {
    id: {
        type: Sequelize.UUID,
        defaultValue: Sequelize.UUIDV4,
        primaryKey: true,
    },
    // job_id: Sequelize.UUID,
    failed: Sequelize.INTEGER.UNSIGNED,
    attempts: Sequelize.INTEGER.UNSIGNED,
    replicates: Sequelize.INTEGER.UNSIGNED,
    completed: Sequelize.INTEGER.UNSIGNED
});

// Module relationships
Task.belongsTo(Job);
Job.hasMany(Task);
Task.hasOne(Client);

// Create the tables
sequelize.sync();

module.exports = {
    // sequelize: sequelize,
    Client: Client,
    Job: Job,
    Task: Task
}
