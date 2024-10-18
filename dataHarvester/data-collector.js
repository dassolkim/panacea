const axios = require('axios').default

process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0' // do not reject about unauthorized connection. tls self-signed certificate error occur in axios (windows)
// shared api
module.exports = { getCatalog, getNextCatalog, getDataset, getSocrata, getOpenDataSoft, getDKANJson, getRealData, getRealDataToFile, getRealDataToFileStream }

function getCatalog(sourceInfo) {
  
    const url = sourceInfo.defaultUrl + "catalog.rdf"

    const result = axios.post(url)
        .then(function (response) {
            const data = response.data
            return data

        }).catch(function (error) {
            console.log(error)
        })
    return result
}

function getDataset(sourceInfo) {

    const url = sourceInfo.defaultUrl + ".rdf"
   
    const result = axios.post(url)
        .then(function (response) {
            const data = response.data
            return data

        }).catch(function (error) {
            console.log(error)
        })
    return result
}

function getNextCatalog(sourceInfo, page) {
    
    const url = sourceInfo.defaultUrl + "catalog.rdf?page=" + page
    

    const result = axios.get(url) // when 405 error occurs, change post to get
        .then(function (response) {
            const data = response.data
            // console.log(data)
            return data

        }).catch(function (error) {
            console.log(error)
        })
    return result
}

function getSocrata(sourceInfo) {
  
    const url = sourceInfo.defaultUrl + "data.json"

    const result = axios.get(url)
        .then(function (response) {
            const data = response.data
            return data

        }).catch(function (error) {
            console.log(error)
        })
    return result
}

function getDKANJson(sourceInfo) {
  
    const url = sourceInfo.defaultUrl + "data.json"

    const result = axios.get(url)
        .then(function (response) {
            const data = response.data
            return data

        }).catch(function (error) {
            console.log(error)
        })
    return result
}

function getOpenDataSoft(sourceInfo) {
  
    const url = sourceInfo.defaultUrl + "/api/v2/catalog/exports/rdf"
    // const url = sourceInfo.defaultUrl + "/api/v2/catalog/exports/json"

    const result = axios.get(url)
        .then(function (response) {
            const data = response.data
            return data

        }).catch(function (error) {
            console.log(error)
        })
    return result
}

async function getRealData(url, contentType) {

    const result = axios.get(url, {headers: {'Content-Type': contentType}})
        .then(function (response) {
            const data = response.data
            return data

        }).catch(function (error) {
            console.log(error)
        })
    return result
}

async function getRealDataToFile(url) {

    const result = axios.get(url, {responseType: 'arraybuffer', timeout: 100000})
        .then(function (response) {
            const data = response.data
            return data

        }).catch(function (error) {
            console.log(error)
            const err_msg = error.code
            const err_check = false
            // console.log(err_msg)
            return err_check
            // return err_msg

        })
    return result
}

async function getRealDataToFileStream(url) {

    const result = axios.get(url, {responseType: 'stream', timeout: 100000})
        .then(function (response) {
            const data = response.data
            return data

        }).catch(function (error) {
            console.log(error)
            const err_msg = error.code
            const err_check = false
            // console.log(err_msg)
            return err_check
            // return err_msg

        })
    return result
}