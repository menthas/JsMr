# Browser-based MapReduce Framework
This project is an attempt to create a distributed MapReduce framework using JavaScript. This framework will let you run MapReduce jobs on clients browsers and on dedicated servers (future work).

### Using AWS
To use AWS S3 for file transfer, you need to create a file called `aws.json` in the `config` folder with following content.
```json
{
    "accessKeyId": "access_key",
    "secretAccessKey": "secret",
    "region": "us-west-2"
}

```

### Project Strucutere
```
.
├── app   (most of the node.js app code)
│   ├── admin_routes.js   (/admin routes and logic)
│   ├── background_tasks.js    (Tasks than run in intervals, cleanups, etc.)
│   ├── routes.js    (API routes and logic)
│   └── storage.js    (ORM and persistence)
├── config
│   ├── app.json    (main configurations)
│   └── index.js    (loads the configurations)
├── index.js    (Setup the node.js app, Restify and loads everything)
├── lib
│   └── utils.js    (A set of commonly used functions)
├── package.json    (npm package info)
└── public    (static files, served directly)
    └── static
        ├── admin   (Holds the admin page code)
        ├── sdk
        │   └── jsmr-0.1.js    (The client side SDK for running MapReduce tasks)
        └── test_client.html    (A sample client page)
```
