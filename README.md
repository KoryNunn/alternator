# Alternator

A simplified API to DynamoDB

## State - Experimental

DynamoDB is hard to figure out and the state of the docs really doesn't help. Use this module at your own risk!

## Usage

```js
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
                    name: 'hash', // A hash is required
                    version: 'range' // A range is optional
                },
                attributes: {
                    name: 'string',
                    version: 'number'
                }
            }
        ]

    );

// Create a user
db.table('users').create({
    item: {
        name: 'bob',
        version: 0
    }
}, callback); // -> righto : item

// Get a user
db.table('users').get({
    key: {
        name: 'bob',
        version: 0
    }
}, callback); // -> righto : item

// Get multiple users by index
db.table('users').findAll({
    key: {
        name: 'bob'
    }
}, callback); // -> righto : [item]

// Search multiple users
db.table('users').scan({
    expression: 'foo = :foo',
    attributeValues: {
        ':foo': 'bar'
    }
}, callback); // -> righto : [item]

// Update a user
db.table('users').update({
    key: {
        name: 'bob',
        version: 0
    }
    item: {
        version: 1
    }
}, callback); // -> righto : item

// Update a user with an expression
db.table('users').update({
    key: {
        name: 'bob',
        version: 0
    }
    expression: 'ADD version :one',
    attributeValues: {
        ':one': 1
    }
}, callback); // -> righto : item

// Remove a user
db.table('users').remove({
    key: {
        name: 'bob',
        version: 0
    }
}, callback); // -> righto : nothing
```
