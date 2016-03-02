# Alternator

A simplified API to DynamoDB

## State - Experimental

DynamoDB is hard to figure out and the state of the docs really doesn't help
Use this module at your own risk.

## Usage

```

var alternator = require('alternator');

// create a db wrapper
var db = alternator(

        // AWS config stuff
        {
            endpoint: new AWS.Endpoint('http://localhost:8000'),
            accessKeyId: "myKeyId",
            secretAccessKey: "secretKey",
            region: "us-east-1"
        },

        // An array of configs for tables you expect to exist and that you want to work with.
        // Tables that don't exist on the DB will be created.
        // Existing tables will be compared, and will throw if they do not match.
        [
            {
                name: 'users',
                key: {
                    hash: ['firstName', 'string'],
                    range: ['age', 'number'] // Optional
                }
            }
        ]

    );

// Create a user
db.tables.users.create({
    firstName:
});

```