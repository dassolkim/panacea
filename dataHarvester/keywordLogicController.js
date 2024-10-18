// const config = require('../../config/openDataConfig')
const dc = require('./data-collector')
const fh = require('../fileHandler/file-handler')
const rp = require('../dataExtractor/rdf-parser')
const path = require('path')
const defaultPath = path.join('C:/Users/master/Development/nodeProject/Panacea', 'data/')
const create = require('../metadataManager/action/create')
const get = require('../metadataManager/action/get')
const uuid = require('uuid')
const mime = require('mime-types')

module.exports = {
    downloadDomainUrls, downloadFormatUrls, downloadSocrataDataset, downloadODSDataset,
    downloadArcGISDataset, downloadAllCatalog, ckanKeywordBasedLogic, socrataKeywordBasedLogic,
    dkanKeywordBasedLogic, arcgisKeywordBasedLogic, odsKeywordBasedLogic
}
const v1 = uuid.v1
async function checkPushlisher(sourceInfo) {
    if (portal = 'US') publisher = 'CKAN'
    if (portal = 'CA') publisher = 'CKAN'
    if (portal = 'UK') publisher = 'CKAN'
    if (portal = 'IE') publisher = 'CKAN'
    if (portal = 'SG') publisher = 'CKAN'
    if (portal = 'CH') publisher = 'CKAN'
    if (portal = 'OK') publisher = 'DKAN_rdf'
    if (portal = 'HHS') publisher = 'DKAN_json'
    if (portal = 'NY') publisher = 'Socrata'
    if (portal = 'SF') publisher = 'Socrata'
    if (portal = 'LC') publisher = 'Opendatasoft'
    if (portal = 'BS') publisher = 'Opendatasoft'

}

// with theme
async function ckanKeywordBasedLogic(sourceInfo, keywords, end) {

    const dataDir = defaultPath
    let key_string = ""
    // console.log(keywords)
    // console.log(keywords.length)
    let keyword_list = []
    let getAll = await get.keyword_getAll()

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
    
    for (let k = 0; k < keywords.length; k++) {
        // console.log(getAll)
        if (getAll.length == 0) {
            let kk = {
                id: v1(),
                title: keywords[k]
            }
            await create.keyword_create(kk)
            keyword_list.push(kk)
            console.log('Error: create keyword again')
        }
        else if (getAll.indexOf(keywords[k] != -1)) {
            let getKeyword = await get.keyword_get({ title: keywords[k] })
            let tmp_key = {
                id: getKeyword.getDataValue('id'),
                title: getKeyword.getDataValue('title')
            }
            keyword_list.push(tmp_key)
        }
        else {
            let kk = {
                id: v1(),
                title: keywords[k]
            }
            await create.keyword_create(kk)
            keyword_list.push(kk)
        }
    }
    
    
    // console.log(keyword_list)
    console.log(`key_string: ${key_string}`)

    const urlInfo = {
        name: sourceInfo.name,
        type: 'domain',
        keywords: key_string,
        publisher: sourceInfo.publisher
    }
    let total_count = 0
    for (let page = 2; page <= end; page++) { // test for theme page start 1 --> 2
        sourceInfo.page = page
        const catalog = await fh.readCatalog(dataDir, sourceInfo)
        // console.log(catalog)
        if (!catalog) {
            console.log(`read data is failed`)
        } else {
            const parseData = await rp.catalogParser(catalog)
            // console.log(parseData)
            const dataset = await rp.datasetParser(parseData)
            const keyword = await rp.keywordDatasetParser(dataset, keywords)
            console.log(`keyword based dataset count: ${keyword.length}`)
            let resoucreUrls = []
            let themeList = []
            // console.log(keyword)
            for (let t = 0; t < keyword.length; t++) {

                let temp = keyword[t]
                const _dataset = temp.dataset
                _dataset.id = v1()
                _dataset.publisher = sourceInfo.realPublisher
                let _distribution = {
                    urls: temp.distribution,
                    dataset_id: _dataset.id,
                    title: _dataset.title
                }
                let key_id
                for (let k = 0; k < keyword_list.length; k++) {
                    if (keyword_list[k]['title'] == _dataset.keyword) {
                        key_id = keyword_list[k]['id']
                    }
                }
                // Add theme
                let t_getAll = await get.theme_getAll()
                if(_dataset.theme != undefined) {
                    if (themeList.length == 0) {
                        let temp_theme = Object.values(_dataset.theme)
                        let t_title = temp_theme.shift()
                        console.log(typeof(temp_theme))
                        if (t_getAll.indexOf(t_title != -1)) {      
                            let getTheme = await get.theme_get({title: t_title})
                            // console.log(getTheme)
                            let tt = {
                                id: getTheme.getDataValue('id'),
                                title: getTheme.getDataValue('title')
                            }
                            themeList.push(tt)
                        }
                        else {
                            let tt = {
                                id: v1(),
                                title: t_title
                            }
                            await create.theme_create(tt)
                            themeList.push(tt)
                        }
                    }
                    // console.log(themeList)
                }
                
                let _datasetKeyword = {
                    id: v1(),
                    dataset_id: _dataset.id,
                    keyword_id: key_id,
                    state: 'active'
                }

                if (themeList.length != 0){
                    _datasetKeyword['theme_id'] = themeList[0]['id']
                }
                console.log(_datasetKeyword)

                // await create.theme_create(_dataset)
                // console.log(_dataset)
                // console.log(_datasetKeyword)

                /**
                 * Please Check delete comment when testing
                 * 
                 */
                await create.dataset_create(_dataset)
                await create.dataset_keyword_create(_datasetKeyword)
                
                if (_distribution.urls.length == undefined) {
                    resoucreUrls.push(Object.values(_distribution))
                }
                else {
                    for (let w = 0; w < _distribution.urls.length; w++) {
                        let k = {
                            url: _distribution.urls[w],
                            dataset_id: _distribution.dataset_id,
                            title: _distribution.title
                        }
                        resoucreUrls.push(Object.values(k))
                    }
                }
            }
            // let urlList = []
            // resoucreUrls.forEach((element) => {
            //     urlList = urlList.concat(element);
            // }) 
            // const rList = await rp.keywordDistributionParser(parseData, urlList)
            // console.log(resoucreUrls)
            const rList = await rp.keywordDistToDatabaseParser(parseData, resoucreUrls)
            // console.log(rList)
            
            console.log(`keyword based distribution parsing results: ${rList.length}`)
            for (let a = 0; a < rList.length; a++) {
                let dist = rList[a]
                dist.id = v1()
                dist.publisher = sourceInfo.realPublisher
                /**
                 * Please Check delete comment when testing
                 * 
                 */
                await create.distribution_create(dist)
            }
            if (page == 1) {
                const schema = rp.schemaParser(parseData)
                // console.log(schema)
            }

            if (rList) {
                const count = rList.length
                const dataset_count = dataset.length
                console.log(`Number of datasets in catalog page ${page}: ${dataset_count}`)
                console.log(`number of including ${keywords} keywords distributions in catalog page ${page}: ${count}`)
                total_count += count
                urlInfo.page = page
                urlInfo.count = count

                const wUrls = await fh.writeDomainUrls(rList, urlInfo)
                if (wUrls) {
                    console.log(`write urls to files is succeeded`)
                }
            }
        }
    }
    console.log(`total ${urlInfo.type} files in ${sourceInfo.publisher} open data portal: ${total_count}`)
    return total_count
}

async function downloadDomainUrls(sourceInfo, keywords, end) {

    const dataDir = defaultPath
    // const format = 'CSV'
    let key_string = ""
    // console.log(keywords)
    // console.log(keywords.length)
    for (let k = 0; k < keywords.length; k++) {
        if (k == 0) key_string = keywords[k]
        else key_string = key_string + '_' + keywords[k]
    }
    console.log(key_string)
    const urlInfo = {
        name: sourceInfo.name,
        type: 'domain',
        keywords: key_string,
        publisher: sourceInfo.publisher
    }
    let total_count = 0
    for (let page = 1; page <= end; page++) {
        sourceInfo.page = page
        const catalog = await fh.readCatalog(dataDir, sourceInfo)
        // console.log(catalog)
        if (!catalog) {
            console.log(`read data is failed`)
        } else {
            const parseData = await rp.catalogParser(catalog)
            const dataset = await rp.datasetParser(parseData)
            const keyword = await rp.keywordParser(dataset, keywords)
            // console.log(keyword.length)
            let resoucreUrls = []
            for (let t = 1; t < keyword.length; t++) {
                let temp = keyword[t]
                if (temp.distribution.length == undefined) {
                    resoucreUrls.push(Object.values(temp.distribution))
                }
                else {
                    for (let w = 0; w < temp.distribution.length; w++) {
                        resoucreUrls.push(Object.values(temp.distribution[w]))
                    }
                }
            }
            let urlList = []
            resoucreUrls.forEach((element) => {
                urlList = urlList.concat(element);
            })
            console.log(urlList.length)
            const rList = await rp.keywordDistributionParser(parseData, urlList)
            // console.log(rList)
            console.log(`keyword based distribution parsing results: ${rList.length}`)

            if (page == 1) {
                const schema = rp.schemaParser(parseData)
                console.log(schema)
            }
            if (rList) {
                const count = rList.length
                const dataset_count = dataset.length
                console.log(`Number of datasets in catalog page ${page}: ${dataset_count}`)
                console.log(`number of including ${keywords} keywords distributions in catalog page ${page}: ${count}`)
                total_count += count
                urlInfo.page = page
                urlInfo.count = count

                const wUrls = await fh.writeDomainUrls(rList, urlInfo)
                if (wUrls) {
                    console.log(`write urls to files is succeeded`)
                }
            }
        }
    }
    console.log(`total ${urlInfo.type} files in ${sourceInfo.publisher} open data portal: ${total_count}`)
    return total_count
}

async function downloadFormatUrls(sourceInfo, format, end) {

    const dataDir = defaultPath

    const urlInfo = {
        name: sourceInfo.name,
        type: 'url',
        format: format,
        publisher: sourceInfo.publisher
    }
    let total_count = 0
    for (let page = 1; page <= end; page++) {
        sourceInfo.page = page
        const catalog = await fh.readCatalog(dataDir, sourceInfo)
        // console.log(catalog)
        if (!catalog) {
            console.log(`read data is failed`)
        } else {
            const parseData = await rp.catalogParser(catalog)

            const dataset = await rp.datasetParser(parseData)

            const urls = await rp.formatDistributionParser(parseData, format)
            // console.log(urls)
            console.log(`format based distribution parsing results: ${urls.length}`)

            if (page == 1) {
                const schema = rp.schemaParser(parseData)
                console.log(schema)
            }
            if (urls) {
                const count = urls.length
                const dataset_count = dataset.length
                console.log(`Number of datasets in catalog page ${page}: ${dataset_count}`)
                console.log(`number of csv files in catalog page ${page}: ${count}`)
                total_count += count
                urlInfo.page = page
                urlInfo.count = count

                const wUrls = await fh.writeUrls(urls, urlInfo)
                if (wUrls) {
                    console.log(`write urls to files is succeeded`)
                }
            }
        }
    }
    console.log(`total ${urlInfo.type} files in ${sourceInfo.publisher} open data portal: ${total_count}`)
    return total_count
}

async function socrataKeywordBasedLogic(sourceInfo, keywords, end) {

    const dataDir = defaultPath
    let key_string = ""
    let keyword_list = []
    let getAll = await get.keyword_getAll()

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
    for (let k = 0; k < keywords.length; k++) {
        if (getAll.indexOf(keywords[k] != -1)) {
            let getKeyword = await get.keyword_get({ title: keywords[k] })
            let tmp_key = {
                id: getKeyword.getDataValue('id'),
                title: getKeyword.getDataValue('title')
            }
            keyword_list.push(tmp_key)
        }
        else {
            let kk = {
                id: v1(),
                title: keywords[k]
            }
            await create.keyword_create(kk)
            keyword_list.push(kk)
        }
    }

    console.log(`key_string: ${key_string}`)

    const urlInfo = {
        name: sourceInfo.name,
        type: 'domain',
        keywords: key_string,
        publisher: sourceInfo.publisher
    }
    let total_count = 0
    for (let page = 1; page <= end; page++) {
        sourceInfo.page = page
        const catalog = await fh.readCatalog(dataDir, sourceInfo)
        // console.log(catalog)
        if (!catalog) {
            console.log(`read data is failed`)
        } else {
            const parseData = JSON.parse(catalog)
            const dataset = parseData.dataset

            const dataset_count = dataset.length
            const keyword = rp.socrataKeywordDatasetParser(parseData, keywords)

            let resoucreUrls = []
            console.log(keyword.length)
            for (let t = 0; t < keyword.length; t++) {

                let temp = keyword[t]
                const _dataset = temp.dataset
                _dataset.id = v1()
                _dataset.publisher = sourceInfo.realPublisher
                let _distribution = {
                    urls: temp.distribution,
                    dataset_id: _dataset.id,
                    title: _dataset.title,
                    identifier: _dataset.identifier
                }
                let key_id
                for (let k = 0; k < keyword_list.length; k++) {
                    if (keyword_list[k]['title'] == _dataset.keyword) {
                        key_id = keyword_list[k]['id']
                    }
                }
                let _datasetKeyword = {
                    id: v1(),
                    dataset_id: _dataset.id,
                    keyword_id: key_id,
                    state: 'active'
                }

                await create.dataset_create(_dataset)
                await create.dataset_keyword_create(_datasetKeyword)
                if (_distribution.urls.length == undefined) {
                    resoucreUrls.push(Object.values(_distribution))
                }
                else {
                    for (let w = 0; w < _distribution.urls.length; w++) {
                        let k = {
                            url: _distribution.urls[w],
                            dataset_id: _distribution.dataset_id,
                            title: _distribution.title,
                            identifier: _distribution.identifier
                        }
                        resoucreUrls.push(Object.values(k))
                    }
                }
            }

            const rList = await rp.socrataKeywordToDatabaseParser(resoucreUrls)
            // console.log(rList.length)
            console.log(`keyword based distribution parsing results: ${rList.length}`)
            for (let a = 0; a < rList.length; a++) {
                let dist = rList[a]
                if (dist.mediatype != null){
                    dist.id = v1()
                    let check = dist.format
                    dist.format = check.toString().toUpperCase()
                    dist.publisher = sourceInfo.realPublisher
                    await create.distribution_create(dist)
                }
                // console.log(dist)
            }

            if (rList) {
                const count = rList.length
                console.log(`Number of datasets in socrata catalog: ${dataset_count}`)
                console.log(`number of ${keywords} files in socrata catalog: ${count}`)
                urlInfo.count = count
                urlInfo.page = page
                total_count = count

                const wUrls = await fh.writeDomainUrls(rList, urlInfo)
                if (wUrls) {
                    console.log(`write urls to files is succeeded`)
                }
            }
        }
    }
    console.log(`total ${urlInfo.type} files in ${sourceInfo.publisher} open data portal: ${total_count}`)
    return total_count
}

async function dkanKeywordBasedLogic(sourceInfo, keywords, end) {

    const dataDir = defaultPath
    let key_string = ""
    let keyword_list = []
    let getAll = await get.keyword_getAll()

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
    for (let k = 0; k < keywords.length; k++) {
        if (getAll.indexOf(keywords[k] != -1)) {
            let getKeyword = await get.keyword_get({ title: keywords[k] })
            let tmp_key = {
                id: getKeyword.getDataValue('id'),
                title: getKeyword.getDataValue('title')
            }
            keyword_list.push(tmp_key)
        }
        else {
            let kk = {
                id: v1(),
                title: keywords[k]
            }
            await create.keyword_create(kk)
            keyword_list.push(kk)
        }
    }

    console.log(`key_string: ${key_string}`)

    const urlInfo = {
        name: sourceInfo.name,
        type: 'domain',
        keywords: key_string,
        publisher: sourceInfo.publisher
    }
    let total_count = 0
    for (let page = 1; page <= end; page++) {
        sourceInfo.page = page
        const catalog = await fh.readCatalog(dataDir, sourceInfo)
        // console.log(catalog)
        if (!catalog) {
            console.log(`read data is failed`)
        } else {
            const parseData = JSON.parse(catalog)
            const dataset = parseData.dataset

            const dataset_count = dataset.length
            const keyword = rp.socrataKeywordDatasetParser(parseData, keywords)
            // console.log(keyword)
            let resoucreUrls = []
            console.log(keyword.length)
            for (let t = 0; t < keyword.length; t++) {

                let temp = keyword[t]
                const _dataset = temp.dataset
                _dataset.id = v1()
                _dataset.publisher = sourceInfo.realPublisher
                let _distribution = {
                    urls: temp.distribution,
                    dataset_id: _dataset.id,
                    title: _dataset.title,
                    identifier: _dataset.identifier
                }
                let key_id
                for (let k = 0; k < keyword_list.length; k++) {
                    if (keyword_list[k]['title'] == _dataset.keyword) {
                        key_id = keyword_list[k]['id']
                    }
                }
                let _datasetKeyword = {
                    id: v1(),
                    dataset_id: _dataset.id,
                    keyword_id: key_id,
                    state: 'active'
                }

                // let getDataset = get.dataset_get({url: _dataset.url})
                // // let id = getDataset.getDataValue('id')
                // if (getDataset == undefined){
                //     let id = getDataset.getDataValue('id')
                //     console.log(`This dataset is already exists: ${id}`)
                //     _dataset.id = null
                // } else {
                //     // console.log(getDataset.getDataValue('id'))
                //     console.log('not null dataset id')
                //     console.log(getDataset)
                // }
                await create.dataset_create(_dataset)
                await create.dataset_keyword_create(_datasetKeyword)
                if (_distribution.urls != undefined) {
                    const count = Object.keys(_distribution.urls).length;

                    if (count == undefined) {
                        resoucreUrls.push(Object.values(_distribution))
                        // console.log(`undefined: count = ${count}`)
                    }
                    else {
                        // console.log(`count = ${count}`)
                        for (let w = 0; w < count; w++) {
                            let k = {
                                url: _distribution.urls[w],
                                dataset_id: _distribution.dataset_id,
                                title: _distribution.title,
                                identifier: _distribution.identifier
                            }
                            resoucreUrls.push(Object.values(k))
                        }
                    }
                }

            }
            // console.log(resoucreUrls)
            const rList = await rp.socrataKeywordToDatabaseParser(resoucreUrls)
            // console.log(rList.length)
            console.log(`keyword based distribution parsing results: ${rList.length}`)
            for (let a = 0; a < rList.length; a++) {
                let dist = rList[a]
                if (dist.mediatype != null){
                    dist.id = v1()
                    let check = dist.format
                    dist.format = check.toString().toUpperCase()
                    dist.publisher = sourceInfo.realPublisher
                    await create.distribution_create(dist)
                }
                // console.log(dist)
            }
            if (rList) {
                const count = rList.length
                console.log(`Number of datasets in socrata catalog: ${dataset_count}`)
                console.log(`number of ${keywords} files in socrata catalog: ${count}`)
                urlInfo.count = count
                urlInfo.page = page
                total_count = count

                const wUrls = await fh.writeDomainUrls(rList, urlInfo)
                if (wUrls) {
                    console.log(`write urls to files is succeeded`)
                }
            }
        }
    }
    console.log(`total ${urlInfo.type} files in ${sourceInfo.publisher} open data portal: ${total_count}`)
    return total_count
}

async function arcgisKeywordBasedLogic(sourceInfo, keywords, end) {

    const dataDir = defaultPath
    let key_string = ""
    let keyword_list = []
    let getAll = await get.keyword_getAll()

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
    for (let k = 0; k < keywords.length; k++) {
        if (getAll.indexOf(keywords[k] != -1)) {
            let getKeyword = await get.keyword_get({ title: keywords[k] })
            let tmp_key = {
                id: getKeyword.getDataValue('id'),
                title: getKeyword.getDataValue('title')
            }
            keyword_list.push(tmp_key)
        }
        else {
            let kk = {
                id: v1(),
                title: keywords[k]
            }
            await create.keyword_create(kk)
            keyword_list.push(kk)
        }
    }

    console.log(`key_string: ${key_string}`)

    const urlInfo = {
        name: sourceInfo.name,
        type: 'domain',
        keywords: key_string,
        publisher: sourceInfo.publisher
    }
    let total_count = 0
    for (let page = 1; page <= end; page++) {
        sourceInfo.page = page
        const catalog = await fh.readCatalog(dataDir, sourceInfo)
        // console.log(catalog)
        if (!catalog) {
            console.log(`read data is failed`)
        } else {
            const parseData = JSON.parse(catalog)
            const dataset = parseData.dataset

            const dataset_count = dataset.length
            const keyword = rp.socrataKeywordDatasetParser(parseData, keywords)

            let resoucreUrls = []
            console.log(keyword.length)
            for (let t = 0; t < keyword.length; t++) {

                let temp = keyword[t]
                const _dataset = temp.dataset
                _dataset.id = v1()
                _dataset.publisher = sourceInfo.realPublisher
                let _distribution = {
                    urls: temp.distribution,
                    dataset_id: _dataset.id,
                    title: _dataset.title,
                    identifier: _dataset.identifier
                }
                let key_id
                for (let k = 0; k < keyword_list.length; k++) {
                    if (keyword_list[k]['title'] == _dataset.keyword) {
                        key_id = keyword_list[k]['id']
                    }
                }
                let _datasetKeyword = {
                    id: v1(),
                    dataset_id: _dataset.id,
                    keyword_id: key_id,
                    state: 'active'
                }

                await create.dataset_create(_dataset)
                await create.dataset_keyword_create(_datasetKeyword)
                if (_distribution.urls.length == undefined) {
                    resoucreUrls.push(Object.values(_distribution))
                }
                else {
                    for (let w = 0; w < _distribution.urls.length; w++) {
                        let k = {
                            url: _distribution.urls[w],
                            dataset_id: _distribution.dataset_id,
                            title: _distribution.title,
                            identifier: _distribution.identifier
                        }
                        resoucreUrls.push(Object.values(k))
                    }
                }
            }

            const rList = await rp.arcgisKeywordToDatabaseParser(resoucreUrls)
            // console.log(rList.length)
            console.log(`keyword based distribution parsing results: ${rList.length}`)
            for (let a = 0; a < rList.length; a++) {
                let dist = rList[a]
                if (dist.mediatype != null){
                    dist.id = v1()
                    let check = dist.format
                    dist.format = check.toString().toUpperCase()
                    dist.publisher = sourceInfo.realPublisher
                    await create.distribution_create(dist)
                }
                console.log(dist)
            }

            if (rList) {
                const count = rList.length
                console.log(`Number of datasets in arcgis catalog: ${dataset_count}`)
                console.log(`number of ${keywords} files in arcgis catalog: ${count}`)
                urlInfo.count = count
                urlInfo.page = page
                total_count = count

                const wUrls = await fh.writeDomainUrls(rList, urlInfo)
                if (wUrls) {
                    console.log(`write urls to files is succeeded`)
                }
            }
        }
    }
    console.log(`total ${urlInfo.type} files in ${sourceInfo.publisher} open data portal: ${total_count}`)
    return total_count
}

async function odsKeywordBasedLogic(sourceInfo, keywords, end) {

    const dataDir = defaultPath
    let key_string = ""
    let keyword_list = []
    let getAll = await get.keyword_getAll()

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
    for (let k = 0; k < keywords.length; k++) {
        if (getAll.indexOf(keywords[k] != -1)) {
            let getKeyword = await get.keyword_get({ title: keywords[k] })
            let tmp_key = {
                id: getKeyword.getDataValue('id'),
                title: getKeyword.getDataValue('title')
            }
            keyword_list.push(tmp_key)
        }
        else {
            let kk = {
                id: v1(),
                title: keywords[k]
            }
            await create.keyword_create(kk)
            keyword_list.push(kk)
        }
    }

    console.log(`key_string: ${key_string}`)

    const urlInfo = {
        name: sourceInfo.name,
        type: 'domain',
        keywords: key_string,
        publisher: sourceInfo.publisher
    }
    let total_count = 0
    for (let page = 1; page <= end; page++) {
        sourceInfo.page = page
        const catalog = await fh.readCatalog(dataDir, sourceInfo)
        // console.log(catalog)
        if (!catalog) {
            console.log(`read data is failed`)
        } else {
            const parseData = JSON.parse(catalog)
            const dataset = parseData.dataset
            // console.log(dataset)
            const dataset_count = dataset.length
            const keyword = rp.odsKeywordDatasetParser(dataset, keywords)
            // console.log(keyword)
            let resoucreUrls = []
            console.log(keyword.length)
            for (let t = 0; t < keyword.length; t++) {

                let temp = keyword[t]
                const _dataset = temp.dataset
                _dataset.id = v1()
                _dataset.publisher = sourceInfo.realPublisher
                let _distribution = {
                    urls: temp.distribution,
                    dataset_id: _dataset.id,
                    title: _dataset.title,
                    identifier: _dataset.url
                }
                let key_id
                for (let k = 0; k < keyword_list.length; k++) {
                    if (keyword_list[k]['title'] == _dataset.keyword) {
                        key_id = keyword_list[k]['id']
                    }
                }
                let _datasetKeyword = {
                    id: v1(),
                    dataset_id: _dataset.id,
                    keyword_id: key_id,
                    state: 'active'
                }

                await create.dataset_create(_dataset)
                await create.dataset_keyword_create(_datasetKeyword)
                // if( t % 100 == 0) {
                //     console.log(_distribution)}
                if (_distribution.urls.length == undefined) {
                    resoucreUrls.push(Object.values(_distribution))
                }
                else {
                    for (let w = 0; w < _distribution.urls.length; w++) {
                        let k = {
                            url: _distribution.urls[w],
                            dataset_id: _distribution.dataset_id,
                            title: _distribution.title,
                            identifier: _distribution.identifier
                        }
                        resoucreUrls.push(Object.values(k))
                    }
                }
            }
            // console.log(resoucreUrls)
            const rList = await rp.odsKeywordToDatabaseParser(resoucreUrls)
            // console.log(rList)
            console.log(`keyword based distribution parsing results: ${rList.length}`)
            let rdist = []
            for (let a = 0; a < rList.length; a++) {
                let dist = rList[a]
                if (dist.url != undefined){
                    dist.id = v1()
                    let check = dist.format
                    dist.format = check.toString().toUpperCase()
                    if(dist.mediatype == undefined){
                        dist.mediatype = mime.lookup(check)
                    }
                    dist.publisher = sourceInfo.realPublisher
                    // console.log(dist)
                    rdist.push(dist)
                    await create.distribution_create(dist)
                }
            }
            console.log(`keyword based real distribution parsing results: ${rdist.length}`)


            if (rdist) {
                const count = rdist.length
                console.log(`Number of datasets in arcgis catalog: ${dataset_count}`)
                console.log(`number of ${keywords} files in arcgis catalog: ${count}`)
                urlInfo.count = count
                urlInfo.page = page
                total_count = count

                const wUrls = await fh.writeDomainUrls(rdist, urlInfo)
                if (wUrls) {
                    console.log(`write urls to files is succeeded`)
                }
            }
        }
    }
    console.log(`total ${urlInfo.type} files in ${sourceInfo.publisher} open data portal: ${total_count}`)
    return total_count
}

async function downloadAllCatalog(sourceInfo, end) {
    /**
     * Get all of US Catalog
     * US end: < 3436
     */
    if (sourceInfo.publisher == "Socrata") {
        const catalog = await dc.getSocrata(sourceInfo)
        console.log(`######### Collect ${sourceInfo.name}, page ${end} on Web (data portal) #########`)
        sourceInfo.page = end
        const dump = JSON.stringify(catalog)
        // console.log(dump)
        // imeplements socrata pagenation
        sourceInfo.publisher = sourceInfo.publisher + '/' + sourceInfo.portal
        const file = await fh.writeCatalog(dump, sourceInfo)
        if (file) {
            console.log(`collecting and storing ${sourceInfo.name} file is succeeded`)
        } else {
            console.log('storing file is failed')
        }
    } else if (sourceInfo.publisher == "ArcGIS") {
        const catalog = await dc.getSocrata(sourceInfo)
        console.log(`######### Collect ${sourceInfo.name}, page ${end} on Web (data portal) #########`)
        sourceInfo.page = end
        const dump = JSON.stringify(catalog)
        // console.log(dump)
        // imeplements socrata pagenation
        sourceInfo.publisher = sourceInfo.publisher + '/' + sourceInfo.portal
        const file = await fh.writeCatalog(dump, sourceInfo)
        if (file) {
            console.log(`collecting and storing ${sourceInfo.name} file is succeeded`)
        } else {
            console.log('storing file is failed')
        }
    } else if (sourceInfo.publisher == "DKAN_json") {
        const catalog = await dc.getDKANJson(sourceInfo)
        console.log(`######### Collect ${sourceInfo.name}, page ${end} on Web (data portal) #########`)
        sourceInfo.page = end
        const dump = JSON.stringify(catalog)
        console.log(dump)
        // imeplements socrata pagenation
        sourceInfo.publisher = sourceInfo.publisher + '/' + sourceInfo.portal

        const file = await fh.writeCatalog(dump, sourceInfo)
        if (file) {
            console.log(`collecting and storing ${sourceInfo.name} file is succeeded`)
        } else {
            console.log('storing file is failed')
        }
    } else if (sourceInfo.publisher == "Opendatasoft") {
        const catalog = await dc.getOpenDataSoft(sourceInfo)
        console.log(`######### Collect ${sourceInfo.name}, page ${end} on Web (data portal) #########`)
        sourceInfo.page = end
        sourceInfo.publisher = sourceInfo.publisher + '/' + sourceInfo.city
        const file = await fh.writeCatalog(catalog, sourceInfo)
        if (file) {
            console.log(`collecting and storing ${sourceInfo.name} file is succeeded`)
        } else {
            console.log('storing file is failed')
        }

    } else if (sourceInfo.publisher == "Junar") {
        const catalog = await dc.getSocrata(sourceInfo)
        console.log(`######### Collect ${sourceInfo.name}, page ${end} on Web (data portal) #########`)
        sourceInfo.page = end
        const dump = JSON.stringify(catalog)
        // console.log(dump)
        // imeplements socrata pagenation
        sourceInfo.publisher = sourceInfo.publisher + '/' + sourceInfo.portal
        const file = await fh.writeCatalog(dump, sourceInfo)
        if (file) {
            console.log(`collecting and storing ${sourceInfo.name} file is succeeded`)
        } else {
            console.log('storing file is failed')
        }
    } else {
        for (let page = 1; page <= end; page++) {
            const catalog = await dc.getNextCatalog(sourceInfo, page)
            console.log(`######### Collect ${sourceInfo.name}, page ${page} on Web (data portal) #########`)
            sourceInfo.page = page
            const file = await fh.writeCatalog(catalog, sourceInfo)
            // console.log(file)
            if (file) {
                console.log(`collecting and storing ${sourceInfo.name} page ${page} file is succeeded`)
            } else {
                console.log('storing file is failed')
            }
        }
    }
}

async function downloadSocrataDataset(sourceInfo, format, page) {

    const dataDir = defaultPath
    sourceInfo.publisher = sourceInfo.publisher + '/' + sourceInfo.portal
    const urlInfo = {
        name: sourceInfo.name,
        type: 'url',
        format: format.toLowerCase(),
        publisher: sourceInfo.publisher,
        page: page
    }
    console.log(`######### Collect ${sourceInfo.name} on Web (data portal) #########`)
    const catalog = await fh.readCatalog(dataDir, sourceInfo)
    let total_count
    if (!catalog) {
        console.log(`read data is failed`)
    } else {
        const parseData = JSON.parse(catalog)
        // console.log(parseData)

        const dataset = parseData.dataset
        // console.log(dataset)
        const dataset_count = dataset.length
        const dist_list = rp.socrataDatasetParser(parseData)
        console.log(dist_list)

        const url_list = []
        let j = 0
        let socrata_format
        // let dist
        if (format == 'CSV') socrata_format = 'text/csv'
        if (format == 'JSON') socrata_format = 'application/json'
        let k = 0
        while (k < dataset_count) {
            const di = dist_list[k]
            if (di != undefined) {
                const dc = di.length
                for (let i = 0; i < dc; i++) {
                    const d = di[i]
                    if (d != undefined) {
                        if (d["mediaType"] == socrata_format) {
                            url_list[j] = d['downloadURL']
                            j++
                        }
                    }
                }
            }
            k++
        }
        if (url_list) {
            const count = url_list.length
            console.log(`Number of datasets in socrata catalog: ${dataset_count}`)
            console.log(`number of ${format} files in socrata catalog: ${count}`)
            urlInfo.count = count
            urlInfo.page = page
            total_count = count

            const wUrls = await fh.writeUrls(url_list, urlInfo)
            if (wUrls) {
                console.log(`write urls to files is succeeded`)
            }
        }
    }
    console.log(`total ${format} files in ${sourceInfo.publisher} open data portal: ${total_count}`)
    return total_count
}

async function downloadArcGISDataset(sourceInfo, format, page) {

    const dataDir = defaultPath
    sourceInfo.publisher = sourceInfo.publisher + '/' + sourceInfo.portal
    const urlInfo = {
        name: sourceInfo.name,
        type: 'url',
        format: format.toLowerCase(),
        publisher: sourceInfo.publisher,
        page: page
    }
    console.log(`######### Collect ${sourceInfo.name} on Web (data portal) #########`)
    const catalog = await fh.readCatalog(dataDir, sourceInfo)
    let total_count
    if (!catalog) {
        console.log(`read data is failed`)
    } else {
        const parseData = JSON.parse(catalog)
        // console.log(parseData)

        const dataset = parseData.dataset
        // console.log(dataset)
        const dataset_count = dataset.length
        const dist_list = rp.socrataDatasetParser(parseData)
        console.log(dist_list)

        const url_list = []
        let j = 0
        let socrata_format
        // let dist
        if (format == 'CSV') socrata_format = 'text/csv'
        if (format == 'JSON') socrata_format = 'application/json'
        let k = 0
        while (k < dataset_count) {
            const di = dist_list[k]
            if (di != undefined) {
                const dc = di.length
                for (let i = 0; i < dc; i++) {
                    const d = di[i]
                    if (d != undefined) {
                        if (d["mediaType"] == socrata_format) {
                            url_list[j] = d['accessURL']
                            j++
                        }
                    }
                }
            }
            k++
        }
        if (url_list) {
            const count = url_list.length
            console.log(`Number of datasets in ArcGIS catalog: ${dataset_count}`)
            console.log(`number of ${format} files in ArcGIS catalog: ${count}`)
            urlInfo.count = count
            urlInfo.page = page
            total_count = count

            const wUrls = await fh.writeUrls(url_list, urlInfo)
            if (wUrls) {
                console.log(`write urls to files is succeeded`)
            }
        }
    }
    console.log(`total ${format} files in ${sourceInfo.publisher} open data portal: ${total_count}`)
    return total_count
}

async function downloadODSDataset(sourceInfo, format, page) {

    const dataDir = defaultPath
    const publisher = sourceInfo.publisher + '/' + sourceInfo.city
    const urlInfo = {
        name: sourceInfo.name,
        type: 'url',
        format: format.toLowerCase(),
        publisher: publisher
    }
    sourceInfo.publisher = publisher
    console.log(`######### Collect ${sourceInfo.name} on Web (data portal) #########`)
    const catalog = await fh.readCatalog(dataDir, sourceInfo)
    let total_count
    // console.log(catalog)
    if (!catalog) {
        console.log(`read data is failed`)
    } else {
        const parseData = await rp.catalogParser(catalog)

        const urls = await rp.odsDatasetParser(parseData, urlInfo.format)
        // console.log(urls)
        if (urls) {
            const count = urls.length
            console.log(`number of csv files in catalog page ${page}: ${count}`)
            urlInfo.page = page
            urlInfo.count = count
            total_count = count

            const wUrls = await fh.writeUrls(urls, urlInfo)
            if (wUrls) {
                console.log(`write urls to files is succeeded`)
            }
        }
    }
    console.log(`total ${format} files in ${sourceInfo.publisher} open data portal: ${total_count}`)
    return total_count
}