const models = require('../../models/index')
const uuid = require('uuid')
const v1 = uuid.v1
module.exports = { dataset_update, distribution_update }


function dataset_update(dataset) {

    // dataset.id = v1()
    let _dataset = {
        // datalake_id: dataset.datalake_id,
        where: {
            id: dataset.id
        }
    }
    const result = models.Dataset.update(_dataset).then(_ => console.log("dataset_create is succeeded"))
    return result
}

function distribution_update(distribution) {

    let value = {
        datalake_id: distribution.datalake_id,
        name: distribution.name,
        migration_status: distribution.migration_status
    }
    let condition = { where: {id: distribution.id}}
    const result = models.Distribution.update(value, condition).then(_ => console.log("distribution_update is succeeded"))
    return result
}

function keyword_create(keyword) {

    // dataset.id = v1()
    const result = models.Keyword.create(keyword).then(_ => console.log("keyword_create is succeeded"))
    return result
}

function dataset_keyword_create(dataset_keyword) {

    // dataset.id = v1()
    const result = models.DatasetKeyword.create(dataset_keyword).then(_ => console.log("keyword_create is succeeded"))
    return result
}
function datalake_create(datalake) {

    // dataset.id = v1()
    const result = models.Datalake.create(datalake).then(_ => console.log("keyword_create is succeeded"))
    return result
}

