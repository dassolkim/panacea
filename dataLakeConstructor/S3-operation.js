const AWS = require('aws-sdk')
const config = require('../config')

s3 = new AWS.S3({
    apiVersion: config.apiVersion,
    accessKeyId: config.accessKeyId,
    secretAccessKey: config.secretAccessKey,
    endpoint: config.endpoint,
    s3ForcePathStyle: config.s3ForcePathStyle,
    sslEnabled: config.sslEnabled,
    signatureVersion: config.signatureVersion
});

async function getBucketList(s3) {
    s3.listBuckets(function (err, data) {
        if (err) {
            console.log("Error", err)
        } else {
            // console.log("Success", data.Buckets)
            // console.log(data)
            let result = data.Buckets
            return result
        }
    })
}

console.log(getBucketList(s3))


// async function createBucket(s3, bucket) {
//     s3.createBucket(bucket, function(err, data) {
//         if (err){
//             console.log("bucket create error", err)
//         } else {
//             console.log("create bucket suceeded", data.Location)
//         }
//     })
// }
// let bucketParams = {
//     Bucket: 'test'
// }
// createBucket(s3, bucketParams)