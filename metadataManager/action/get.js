const models = require('../../models/index')
const uuid = require('uuid')
const v1 = uuid.v1
module.exports = { dataset_getAll, dataset_get, keyword_getAll, keyword_get, distribution_getAll, distribution_get, datalake_get,
    theme_get, theme_getAll, format_get, format_getAll, dataset_url_get, theme_title_get, dataset_identifier_get }


function dataset_getAll() {

    const result = models.Dataset.findAll({ raw: true })
    return result
}

function dataset_get(dataset) {
    let _dataset = {
        attributes: ['id', 'name'],
        order: [['createdAt', 'DESC']],
        where: {
            id: dataset.id
        },
        raw: true
    }
    const result = models.Dataset.findOne({where: {url: dataset.url}})
    return result
}

function dataset_url_get(dataset) {
    
    const result = models.Dataset.findOne({where: {url: dataset.url}})
    return result
}

function dataset_identifier_get(dataset) {
    
    const result = models.Dataset.findOne({where: {url: dataset.identifier}})
    return result
}

function keyword_get(keyword) {

    let _keyword = {
        attributes: ['id', 'title'],
        order: [['createdAt', 'DESC']],
        where: { title: keyword.title },
        raw: true
    }
    // const result = models.Keyword.findOne(_keyword)
    const result = models.Keyword.findOne({where: {title: keyword.title}})

    return result
}

function keyword_getAll() {


    const result = models.Keyword.findAll({ raw: true })
    return result
}

function distribution_getAll() {

    const result = models.Distribution.findAll({ raw: true }).then(_ => console.log("distribution_getAll is succeeded"))
    return result
}

function distribution_get(distribution) {
    let _distribution = {
        attributes: ['id', 'url', 'dataset_id', 'datalake_id', 'name'],
        order: [['createdAt', 'DESC']],
        where: { id: distribution.id }
    }
    const result = models.Distribution.findOne(_distribution, {raw: true})
    // console.log(result)
    return result
}

function datalake_get(datalake) {

    let _datalake = {
        attributes: ['id', 'bucket', 'state', 'type'],
        order: [['createdAt', 'DESC']],
        where: { bucket: datalake.bucket },
        raw: true
    }
    const result = models.Datalake.findOne({where: {bucket: datalake.bucket}})
    return result
}

function dataset_format_get(dataset_format) {

    const result = models.DatasetFormat.findOne({where: {id: dataset_format.id}})
    return result
}

function format_get(format) {

    const result = models.Format.findOne({where: {domain: format.domain}})
    return result
}

function format_getAll() {

    const result = models.Format.findAll({ raw: true })
    return result
}

function theme_get(theme) {

    let _theme = {
        attributes: ['id', 'title'],
        order: [['createdAt', 'DESC']],
        where: { title: theme.title },
        raw: true
    }
    // const result = models.Keyword.findOne(_keyword)
    const result = models.Theme.findOne({where: {title: theme}})
    return result
}

function theme_getAll() {

    const result = models.Theme.findAll({ raw: true })
    return result
}

function theme_title_get(theme) {
    // console.log(`log on get: ${theme}`)
    const result = models.Theme.findOne({where: {title: theme}})
    return result
}

// console.log(keyword_getAll())
// console.log(dataset_getAll())
// console.log(distribution_get({id: '17c616d0-597f-11ed-9994-25eda416914e' }))

// async function test() {
//     const dist = await distribution_get({ id: '17c616d0-597f-11ed-9994-25eda416914e' })
//     console.log(dist)
// }
// test()