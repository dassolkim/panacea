const S3Client = require('./S3-client')
const path = require('path')
const defaultPath = path.join('C:/Users/kimds/nodeProject', 'data/')
const fh = require('../fileHandler/file-handler')
const dc = require('../dataHarvester/data-collector')
const create = require('../metadataManager/action/create')
const update = require('../metadataManager/action/update')
const get = require('../metadataManager/action/get')
const uuid = require('uuid')

module.exports = { createDomainDataLakes, createFormatDataLakes }
// const aycnHook = require('async_hooks')
// const myHook = aycnHook.createHook({
//     init,
//     before,
//     after,
//     destroy,
//     promiseResolve
// })
// myHook.enable()

async function createDomainDataLakes(sourceInfo, keywords, bucket) {
    const urlInfo = sourceInfo
    urlInfo.type = 'domain'
    const dataDir = defaultPath
    let global_cnt = 0
    let original_cnt = 0
    let err_cnt = 0
    let suc_cnt = 0
    let err_list = []
    let control_cnt = 0
    let control_status = false
    let url_info
    const rp = 10 // if ckan then change 100 end page else fix [1]
    this.s3 = new S3Client()
    let key_string = ""
    for (let k = 0; k < keywords.length; k++) {
        if (k == 0) {
            let string = keywords[k]
            key_string = string.replace(/-/g, '')
        }
        else {
            let string = keywords[k]
            key_string = key_string + '-' + string.replace(/-/g, '')
        }
    }
    console.log(key_string)
    await this.s3.connect()
    let _datalake = {}
    /**
     * bucket name change
     */
    const new_bucket = key_string + '/' + bucket
    // const new_bucket = bucket + '/' + key_string
    // const new_bucket = 'climate/climate-emergency'
    let getDatalake = await get.datalake_get({ bucket: new_bucket })
    console.log(`check query raw reaturn: ${getDatalake}`)

    let check_lake
    if (getDatalake != null) {
        check_lake = getDatalake.getDataValue('bucket')
        console.log(check_lake)
        if (check_lake == new_bucket) {
            _datalake = {
                id: getDatalake.getDataValue('id'),
                bucket: getDatalake.getDataValue('bucket')
            }
            console.log(`The bucket name ${check_lake} is already exist`)
        }
    } else {
        _datalake = {
            id: uuid.v1(),
            bucket: new_bucket,
            type: 'keyword',
            state: 'active'
        }
        await create.datalake_create(_datalake)
        console.log('create new_bucket')
    }
    for (let p = 101; p < 102; p++) { // change value [if ckan (p=1; p<rp) if other platform (p=0, p<rp.length)]
        const pg = p // it not ckan then pg = rp[p] 
        urlInfo.page = pg
        // const name = `${sourceInfo.site}_p${pg}_${key_string}_`
        const name = `${sourceInfo.site}_p${pg}_`

        const readUrls = fh.readDomainUrls(urlInfo, key_string)
        const urlObj = JSON.parse(readUrls)
        if (urlObj == undefined || urlObj == false) {continue}
        console.log(urlObj.info)
        // console.log(new_bucket)
        const count = urlObj.info.count
        console.log(`Number of ${key_string} file in ${urlInfo.publisher} portal catalog page ${urlInfo.page}: ${count}`)
        let i = 0 // 555: HHS health-test holding
        let j = 0
        let cnt = 0
        original_cnt += count
        console.time("Total excution Time")
        /**
         * control total count logic
         */
        if (control_status == true) { break }
        // console.time("Total excution Time")
        while (i < count) {
            const obj = urlObj.urls[i]
            j = i + 1
            const url = obj.url
            const mediaType = obj.mediatype
            const format = obj.format
            let title = obj.title
            let obj_title = "-"
            /**
             * control total count logic
             */
            control_cnt++
            if (control_cnt > 250) { 
                control_status = true
                break;
            }
            if (title != undefined){
                let new_title = title.replace(/\s{1,}/gi, "-")
                let renew_title = new_title.replace(/,/g, '')
                if (renew_title.length > 80){
                    let t = renew_title.substr(0, 79)
                    obj_title = t
                } else { obj_title = renew_title}
            }
            const object_name = name + format + '_' + obj_title + '_' + j

            // const object_name = name + '_' + format + '_' + obj_title + '_' + j
            // console.log(data)
            // const data = await dc.getRealData(url, mediaType)
            // console.log(url)
            console.log(`Start migrate real file to datalake`)
            const data = await dc.getRealDataToFile(url)
            // console.log(data)
            // const result = this.s3.putObject(_datalake.bucket, object_name, JSON.stringify(data), mediaType)
            console.log(object_name)
            let err_msg
            if (data == 'ECONNRESET' || data == 'ETIMEDOUT' || data == false) {
                err_msg = data
                err_cnt++
                obj.status = "Failure"
                obj.err_msg = err_msg
                const _distribution = {
                    id: obj.id,
                    datalake_id: _datalake.id,
                    name: object_name,
                    migration_status: obj.status
                }
                err_list.push(obj)
                let dist = get.distribution_get({ id: _distribution.id })
                // console.log(dist)
                let check_dist = dist.datalake_id
                console.log(`distribution get results: ${check_dist}`)
                if (check_dist != null || check_dist == undefined) {
                    await update.distribution_update(_distribution)
                    console.log("migration failed and update metadata ")
                }
            } else {
                const result = this.s3.putObject(_datalake.bucket, object_name, data, mediaType)
                if (result != null) {

                    cnt++
                    suc_cnt++
                    const _distribution = {
                        id: obj.id,
                        datalake_id: _datalake.id,
                        name: object_name,
                        migration_status: "Success"
                    }
                    let dist = get.distribution_get({ id: _distribution.id })
                    // console.log(dist)
                    let check_dist = dist.datalake_id
                    // console.log(`distribution get results: ${check_dist}`)
                    if (check_dist != null || check_dist == undefined) {
                        await update.distribution_update(_distribution)
                        console.log("migration success and update metadata ")
                    }
                }
            }
            i++
        }
        console.timeEnd("Total excution Time")
        global_cnt += cnt
    }
    const statistics = {
        info: urlInfo,
        total: original_cnt,
        success: suc_cnt,
        failed: err_cnt
    }
    urlInfo['keywords'] = key_string
    await fh.writeKeywordMigrationLog(statistics, err_list, urlInfo)
    
    console.log(`Number of ${key_string} files in ${sourceInfo.publisher} portal: ${original_cnt}`)
    console.log(`Number of created sources in ${sourceInfo.publisher} portal ${key_string} files: ${global_cnt}`)
}

async function createFormatDataLakes(sourceInfo, keywords, format, bucket) {
    const urlInfo = sourceInfo
    // let format = format.toLowerCase()
    urlInfo.type = 'format'
    urlInfo.format = format
    const dataDir = defaultPath
    let global_cnt = 0
    let original_cnt = 0
    let err_cnt = 0
    let suc_cnt = 0
    let err_list = []
    let url_info
    const rp = 100 // []
    this.s3 = new S3Client()
    let key_string = ""
    for (let k = 0; k < keywords.length; k++) {
        if (k == 0) {
            let string = keywords[k]
            key_string = string.replace(/-/g, '')
        }
        else {
            let string = keywords[k]
            key_string = key_string + '-' + string.replace(/-/g, '')
        }
    }
    console.log(key_string)
    await this.s3.connect()
    let _datalake = {}
    const new_bucket = key_string + '-' + format + '/' + bucket
    console.log(new_bucket)
    // add logic getBucket and create bucket
    // if (new_bucket == bb) {}
    let getDatalake = await get.datalake_get({ bucket: new_bucket })
    console.log(`check query raw reaturn: ${getDatalake}`)

    let check_lake
    if (getDatalake != null) {
        check_lake = getDatalake.getDataValue('bucket')
        console.log(check_lake)
        if (check_lake == new_bucket) {
            _datalake = {
                id: getDatalake.getDataValue('id'),
                bucket: getDatalake.getDataValue('bucket')
            }
            console.log(`The bucket name ${check_lake} is already exist`)
        }
    } else {
        _datalake = {
            id: uuid.v1(),
            bucket: new_bucket,
            type: 'keyword',
            state: 'active'
        }
        await create.datalake_create(_datalake)
        console.log('create new_bucket')
    }
    // const new_bucket = _datalake.bucket
    // console.log(_datalake)
    // check bucket already exists, add null option
    // if(check_lake == bucket){
    //     _datalake = get_datalake
    //     console.log(`The bucket name ${bucket} is already exist`)
    // } else {
    //     _datalake = {
    //         id: uuid.v1(),
    //         bucket: key_string + '/' + 'us-ckan',
    //         type: 'keyword',
    //         state: 'active'
    //     }
    //     await create.datalake_create(_datalake)
    //     console.log('new_bucket')
    // }

    for (let p = 1; p <= rp; p++) {
        const pg = p
        urlInfo.page = pg
        const name = `${sourceInfo.site}_p${pg}_${key_string}_`
        const readUrls = fh.readFormatUrls(urlInfo, key_string)
        const urlObj = JSON.parse(readUrls)
        console.log(urlObj)
        if (urlObj == undefined || urlObj == false) {continue}
        const count = urlObj.info.count
        url_info = urlObj.info
        console.log(`Check URL info local to global variable: ${url_info}`)
        console.log(`Number of ${key_string} file in ${urlInfo.publisher} portal catalog page ${urlInfo.page}: ${count}`)
        let i = 0
        let j = 0
        let cnt = 0
        original_cnt += count
        console.time("Total excution Time")
        while (i < count) {
            const obj = urlObj.urls[i]
            j = i + 1
            const url = obj.url
            const mediaType = obj.mediatype
            const format = obj.format

            let title = obj.title
            let new_title = title.replace(/\s{1,}/gi, "-")
            let renew_title = new_title.replace(/,/g, '')
            let obj_title
            if (renew_title.length > 80){
                let t = renew_title.substr(0, 79)
                obj_title = t
            } else { obj_title = renew_title}
            // const object_name = name + format + '_' + obj_title + '_' + j
            const object_name = name + obj_title + '_' + j

            // let title = obj.title
            // let new_title = title.replace(/\s{1,}/gi, "-")
            // const object_name = name + format + '_' + new_title + '_' + j
            // console.log(data)
            // const data = await dc.getRealData(url, mediaType)
            // console.log(url)
            console.log(`Start migrate real file to datalake`)
            const data = await dc.getRealDataToFile(url)
            // console.log(data)
            // const result = this.s3.putObject(_datalake.bucket, object_name, JSON.stringify(data), mediaType)
            console.log(object_name)
            let err_msg
            if (data == 'ECONNRESET') {
                err_msg = data
                err_cnt++
                obj.status = "Failure"
                obj.err_msg = err_msg
                const _distribution = {
                    id: obj.id,
                    datalake_id: _datalake.id,
                    name: object_name,
                    migration_status: obj.status
                }
                err_list.push(obj)
                let dist = get.distribution_get({ id: _distribution.id })
                // console.log(dist)
                let check_dist = dist.datalake_id
                console.log(`distribution get results: ${check_dist}`)
                if (check_dist != null || check_dist == undefined) {
                    await update.distribution_update(_distribution)
                    console.log("migration failed and update metadata ")
                }
            } else {
                const result = this.s3.putObject(_datalake.bucket, object_name, data, mediaType)
                if (result != null) {

                    cnt++
                    suc_cnt++
                    const _distribution = {
                        id: obj.id,
                        datalake_id: _datalake.id,
                        name: object_name,
                        migration_status: "Success"
                    }
                    let dist = get.distribution_get({ id: _distribution.id })
                    // console.log(dist)
                    let check_dist = dist.datalake_id
                    // console.log(`distribution get results: ${check_dist}`)
                    if (check_dist != null || check_dist == undefined) {
                        await update.distribution_update(_distribution)
                        console.log("migration success and update metadata ")
                    }
                }
            }
            i++
        }
        console.timeEnd("Total excution Time")
        global_cnt += cnt
    }
    const statistics = {
        info: urlInfo,
        total: original_cnt,
        success: suc_cnt,
        failed: err_cnt
    }
    urlInfo['keywords'] = key_string
    await fh.writeMigrationLog(statistics, err_list, urlInfo)

    console.log(`Number of ${key_string} files in ${sourceInfo.publisher} portal: ${original_cnt}`)
    console.log(`Number of created sources in ${sourceInfo.publisher} portal ${key_string} files: ${global_cnt}`)
}