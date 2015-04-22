# Browser-based MapReduce Framework
This project is an attempt to create a distributed MapReduce framework using JavaScript. This framework will let you run MapReduce jobs on clients browsers and on dedicated servers (future work).

## Requirments
1. Node.js version 0.10.36+
2. npm package manager

## Installation
To install and run a server instance first clone or download this repository. then in the top level directory of this project:
```bash
$ npm install
```
this will install the requirements. Then you need to create an AWS configuration file called `aws.json` in the `config` folder with following content.
```json
{
    "accessKeyId": "access_key",
    "secretAccessKey": "secret",
    "region": "us-west-2"
}
```
You should now be all set! run the server using:
```bash
$ node index.js
```
which should show something like `JsMr listening at http://0.0.0.0:8080`

## Basic Usage
To bring up the admin page simply visit `http://localhost:8080/static/admin/index.html`. This page is self explanetory and gives you some options to setup and monitor jobs. (look into `/jobs` for the job schema and some examples).

To spin up clients than run you jobs point any modern web browser at `http://localhost:8080/static/test_client.html` and check the developmnet console for all sorts of logs.

An example of embedding the SDK on your own pages:
```html
<!doctype html>
<html lang="en-US">
    <head>
        <meta charset="utf-8">
        <title>JsMr Test Page</title>
    
        <script src="./sdk/jsmr-0.1.js" type="text/javascript" charset="utf-8"></script>
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <!--[if lt IE 9]>
            <script src="//cdnjs.cloudflare.com/ajax/libs/html5shiv/3.6.2/html5shiv.js"></script>
        <![endif]-->
    </head>
    <body>
        <script type="text/javascript">
        var jsmr = new JsMr();
        </script>
    </body>
</html>
```
That's it ! the code is well documented so feel free to look around.

### Project Strucutere
```
..
├── app   (App, routes and persistance logic)
│   ├── admin_routes.js
│   ├── background_tasks.js
│   ├── routes.js
│   └── storage.js
├── config   (Configurations and their loader)
│   ├── app.json
│   ├── aws.json
│   └── index.js
├── index.js
├── jobs   (sample jobs)
│   ├── ...
├── lib
│   ├── commons.js   (common functionality)
│   ├── job.js    (all things related to handing jobs and tasks)
│   └── utils.js   (some utilities used in code)
├── package.json
├── public   (things that are served directly to the clients)
│   └── static
│       ├── sdk
│       │   └── jsmr-0.1.js   (SDK file, proabably want to include this on pages)
│       └── test_client.html   (A simple test client)
└── README.md

```
