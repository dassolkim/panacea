const AWS = require('aws-sdk')
const config = require('../config')

class S3Client {
    constructor() {
        //super()
        this.s3 = null;
    }

    async connect() {
        this.s3 = new AWS.S3({
            apiVersion: config.apiVersion,
            accessKeyId: config.accessKeyId,
            secretAccessKey: config.secretAccessKey,
            endpoint: config.endpoint,
            s3ForcePathStyle: config.s3ForcePathStyle,
            sslEnabled: config.sslEnabled,
            signatureVersion: config.signatureVersion
        });
    }

    async getObject(Bucket, Key) {

        const params = {
            Bucket,
            Key
        }
        return new Promise((resolve, reject) => {
            this.s3.getObject(params, (err, data) => {
                if (err) return reject(err);
                return resolve(data)
            })
        })

    }

    async putObject(Bucket, Key, Body, ContentType) {

        const params = {
            Bucket,
            Key,
            Body,
            ContentType,
        };

        return new Promise((resolve, reject) => {
            this.s3.putObject(params, err => {
                if (err) return reject(err);
                // console.log(Key)
                return resolve(Key);
            });
        });
    }

    async getBucket() {

        return new Promise((resolve, reject) => {
            this.s3.getBucket((err, data) => {
                if (err) return reject(err);
                return resolve(data)
            })
        })
    }
}

// s3 = new S3Client()

// s3.getBucket

module.exports = S3Client