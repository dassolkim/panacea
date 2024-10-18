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
// const { exit } = require('process')

module.exports = {
    downloadFormatUrls, downloadSocrataDataset, downloadODSDataset,
    downloadArcGISDataset, downloadAllCatalog, ckanFormatBasedLogic, ckanKeywordBasedLogic, socrataKeywordBasedLogic, socrataFormatBasedLogic,
    dkanKeywordBasedLogic, arcgisKeywordBasedLogic, odsKeywordBasedLogic, arcgisFormatBasedLogic
}
const v1 = uuid.v1

// with theme
async function ckanFormatBasedLogic(sourceInfo, format, keywords, end) {

    const dataDir = defaultPath
    let key_string = ""
    let keyword_list = []
    let getAll = await get.format_getAll()
    let dTheme = await get.theme_get('Health')
    let defaultTheme = {
        id: dTheme.getDataValue('id'),
        title: dTheme.getDataValue('title')
    }
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

    console.log(`This is format getAll results: ${getAll}`)
    for (let a = 0; a < getAll.length; a++) {
        if (getAll.length == 0) {
            let kk = {
                id: v1(),
                title: format,
                domain: key_string
            }
            console.log('Format is nothing exist')
            await create.format_create(kk)
            keyword_list.push(kk)
            console.log('Error: create keyword again')
        }
        else if (getAll[a]['domain'] == key_string) {
            let getDomain = await get.format_get({ domain: key_string })
            let tmp_format = {
                id: getDomain.getDataValue('id'),
                title: getDomain.getDataValue('title'),
                domain: getDomain.getDataValue('domain')
            }
            keyword_list.push(tmp_format)
        }
        else {
            let kk = {
                id: v1(),
                domain: key_string,
                title: format
            }
            await create.format_create(kk)
            keyword_list.push(kk)
        }
    }
    // console.log(keyword_list)
    console.log(`key_string: ${key_string}`)

    const urlInfo = {
        name: sourceInfo.name,
        type: 'format',
        keywords: key_string,
        format: format,
        publisher: sourceInfo.publisher
    }
    let total_count = 0
    for (let page = 1; page <= end; page++) { // test for theme page start 1 --> 2
        sourceInfo.page = page
        const catalog = await fh.readCatalog(dataDir, sourceInfo)
        // console.log(catalog)
        if (!catalog) {
            console.log(`read data is failed`)
        } else {
            const parseData = await rp.catalogParser(catalog)
            // console.log(parseData)
            const dataset = await rp.datasetParser(parseData)
            // console.log(dataset)
            // const keyword = await rp.keywordDatasetParser(dataset, keywords)
            const keyword = await rp.keywordDatasetParserUpper(dataset, keywords)
            console.log(`keyword based dataset count: ${keyword.length}`)
            // console.log(keyword)
            let resoucreUrls = []
            let themeList = []
            // console.log(keyword)
            for (let t = 0; t < keyword.length; t++) {

                let temp = keyword[t]
                const _dataset = temp.dataset
                let exist = await get.dataset_url_get({ url: _dataset.url })
                // if (exist != null) { continue }
                // else {
                _dataset.id = v1()
                _dataset.publisher = sourceInfo.realPublisher

                let _distribution = {
                    urls: temp.distribution,
                    dataset_id: _dataset.id,
                    title: _dataset.title
                }

                let format_id = keyword_list[0]['id']
                // console.log(format_id)
                // Add theme
                let t_getAll = await get.theme_getAll()
                // console.log(t_getAll)
                // console.log(t_getAll.length)
                let t_title
                if (_dataset.theme != undefined) {
                    let temp_theme = Object.values(_dataset.theme)
                    t_title = temp_theme.shift()
                    let exist_theme = []
                    for (let a = 0; a < t_getAll.length; a++) {
                        const check = t_getAll[a]['title']
                        console.log(check)
                        if (check == t_title) {
                            exist_theme.push(t_title)
                        }
                    }
                    if (exist_theme.indexOf(t_title) != -1) {
                        let getTheme = await get.theme_get(t_title)
                        console.log(`Return getTheme values: ${getTheme}`)
                        let tt = {
                            id: getTheme.getDataValue('id'),
                            title: getTheme.getDataValue('title')
                        }
                        themeList.push(tt)
                    } else {
                        let tt = {
                            id: v1(),
                            title: t_title
                        }
                        await create.theme_create(tt)
                        themeList.push(tt)
                    }
                }
                let _datasetFormat = {
                    id: v1(),
                    dataset_id: _dataset.id,
                    format_id: format_id,
                    state: 'active'
                }
                // console.log(`ThmeList is : ${themeList}`)
                // console.log(defaultTheme)
                if (themeList.length != 0) {
                    _datasetFormat['theme_id'] = themeList[0]['id']
                    _dataset['theme'] = themeList[0]['title']
                } else {
                    _datasetFormat['theme_id'] = defaultTheme.id
                    _dataset['theme'] = defaultTheme.title
                }

                // console.log(_dataset)
                // console.log(_datasetFormat)

                /**
                 * Please Check delete comment when testing
                 * 
                 */
                await create.dataset_create(_dataset)
                await create.dataset_format_create(_datasetFormat)
                if (page == 94) console.log(_distribution)
                // 
                console.log(`catalog index: ${page}`)
                if (_distribution.urls == undefined) continue
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
                let u_format = format.toUpperCase()
                // console.log(u_format)
                const rList = await rp.formatDistToDatabaseParser(parseData, u_format, resoucreUrls)
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

                if (rList) {
                    const count = rList.length
                    const dataset_count = dataset.length
                    console.log(`Number of datasets in catalog page ${page}: ${dataset_count}`)
                    console.log(`number of including ${keywords} keywords distributions in catalog page ${page}: ${count}`)
                    total_count += count
                    urlInfo.page = page
                    urlInfo.count = count
                    urlInfo.format = format.toLowerCase()
                    // console.log(rList)
                    const wUrls = await fh.writeFormatUrls(rList, urlInfo)
                    if (wUrls) {
                        console.log(`write urls to files is succeeded`)
                    }
                }
            }
        }
    }
    console.log(`total ${urlInfo.type} files in ${sourceInfo.publisher} open data portal: ${total_count}`)
    return total_count
}

async function ckanKeywordBasedLogic(sourceInfo, keywords, end) {

    const dataDir = defaultPath
    let key_string = ""
    let keyword_list = []
    let dTheme = await get.theme_get('Health')
    let defaultTheme = {
        id: dTheme.getDataValue('id'),
        title: dTheme.getDataValue('title')
    }
    let getAll = await get.keyword_getAll()
    // sourceInfo.publisher = 
    let total_dataset = 0
    let total_distribution = 0
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
    /**
     * check existing keywords
     */
    let exist_keyword = []
    for (let a = 0; a < getAll.length; a++) {
        const check = getAll[a]['title']
        // console.log(check)
        for (let k = 0; k < keywords.length; k++) {
            if (check == keywords[k]) {
                exist_keyword.push(keywords[k])
            }
        }
    }
    // console.log(exist_keyword)
    for (let k = 0; k < keywords.length; k++) {
        if (exist_keyword.indexOf(keywords[k]) != -1) {
            let getKeyword = await get.keyword_get({ title: keywords[k] })
            let tmp_key = {
                id: getKeyword.getDataValue('id'),
                title: getKeyword.getDataValue('title')
            }
            keyword_list.push(tmp_key)
        } else {
            let kk = {
                id: v1(),
                title: keywords[k]
            }
            await create.keyword_create(kk)
            keyword_list.push(kk)
        }
    }

    const urlInfo = {
        name: sourceInfo.name,
        type: 'domain',
        keywords: key_string,
        publisher: sourceInfo.publisher
    }
    let total_count = 0
    for (let page = 11; page <= end; page++) { // test for theme page start 1 --> 2
        sourceInfo.page = page
        const catalog = await fh.readCatalog(dataDir, sourceInfo)
        // console.log(catalog)
        if (!catalog) {
            console.log(`read data is failed`)
        } else {
            const parseData = await rp.catalogParser(catalog)
            // console.log(parseData)
            const dataset = await rp.datasetParser(parseData)
            // console.log(dataset)
            // const keyword = await rp.keywordDatasetParser(dataset, keywords)
            const keyword = await rp.keywordDatasetParserUpper(dataset, keywords)
            console.log(`keyword based dataset count: ${keyword.length}`)
            let d_count = keyword.length
            total_dataset += d_count
            // console.log(keyword)
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
                /**
                 * Add theme
                 * Remove for clear, fill default Theme
                 */
                // let t_getAll = await get.theme_getAll()
                // // console.log(t_getAll)
                // // console.log(t_getAll.length)
                // let t_title
                // if (_dataset.theme != undefined) {
                //     let temp_theme = Object.values(_dataset.theme)
                //     t_title = temp_theme.shift()
                //     let exist_theme = []
                //     for (let a = 0; a < t_getAll.length; a++) {
                //         const check = t_getAll[a]['title']
                //         console.log(check)
                //         if (check == t_title) {
                //             exist_theme.push(t_title)
                //         }
                //     }
                //     if (exist_theme.indexOf(t_title) != -1){
                //         let getTheme = await get.theme_get(t_title)
                //             console.log(`Return getTheme values: ${getTheme}`)
                //             let tt = {
                //                 id: getTheme.getDataValue('id'),
                //                 title: getTheme.getDataValue('title')
                //             }
                //             themeList.push(tt)
                //     } else {
                //         let tt = {
                //             id: v1(),
                //             title: t_title
                //         }
                //         if (typeof(t_title) == 'string'){
                //             await create.theme_create(tt)
                //             themeList.push(tt)
                //         }
                //     }
                // }
                let _datasetKeyword = {
                    id: v1(),
                    dataset_id: _dataset.id,
                    keyword_id: key_id,
                    state: 'active'
                }
                // console.log(`ThmeList is : ${themeList}`)
                // console.log(defaultTheme)

                if (themeList.length != 0) {
                    _dataset['theme'] = themeList[0]['title']
                } else {
                    _dataset['theme'] = defaultTheme.title
                }
                if (_dataset.identifier == undefined) _dataset.identifier = _dataset.url
                // console.log(_dataset)
                // console.log(_datasetKeyword)

                /**
                 * Please Check delete comment when testing
                 * 
                 */
                // await create.dataset_create(_dataset)
                // await create.dataset_keyword_create(_datasetKeyword)

                // if (page == 94) console.log(_distribution)
                if (_distribution.urls == undefined) continue
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
            } // checkpoint
            // let u_format = 'CSV'
            // const rList = await rp.formatDistToDatabaseParser(parseData, u_format, resoucreUrls)
            // console.log(resoucreUrls)
            total_distribution += resoucreUrls.length
            const rList = await rp.keywordDistToDatabaseParser(parseData, resoucreUrls)
            // console.log(rList)
            console.log(`catalog index: ${page}`)
            console.log(`keyword based distribution parsing results: ${rList.length}`)
            for (let a = 0; a < rList.length; a++) {
                let dist = rList[a]
                dist.id = v1()
                dist.publisher = sourceInfo.realPublisher
                let idf = dist.identifier
                if (idf.length > 250) {
                    dist.identifier = idf.substr(0, 249)
                }
                if (dist.url.length > 254) { continue }
                /**
                 * Please Check delete comment when testing
                 * 
                 */
                await create.distribution_create(dist)
            }

            if (rList) {
                const count = rList.length
                // total_distribution += count
                const dataset_count = dataset.length
                const keyword_data_count = keyword.length
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
            // console.log(key_string)
            // } // checkpoint
        }
    }
    // console.log(keyword_list)
    console.log(keyword_list)
    console.log(`number of including ${keywords} keywords original distributions in whole portal: ${total_distribution}`)
    console.log(`number of including ${keywords} keywords datasets in whole portal: ${total_dataset}`)
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
                console.log(urls)
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
    let check_dup_cnt = 0
    // sourceInfo.publisher = 
    let total_dataset = 0
    let total_distribution = 0
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
    /**
     * check existing keywords
     */
    let exist_keyword = []
    for (let a = 0; a < getAll.length; a++) {
        const check = getAll[a]['title']
        console.log(check)
        for (let k = 0; k < keywords.length; k++) {
            if (check == keywords[k]) {
                exist_keyword.push(keywords[k])
            }
        }
    }
    console.log(exist_keyword)
    for (let k = 0; k < keywords.length; k++) {
        if (exist_keyword.indexOf(keywords[k]) != -1) {
            let getKeyword = await get.keyword_get({ title: keywords[k] })
            let tmp_key = {
                id: getKeyword.getDataValue('id'),
                title: getKeyword.getDataValue('title')
            }
            keyword_list.push(tmp_key)
        } else {
            let kk = {
                id: v1(),
                title: keywords[k]
            }
            await create.keyword_create(kk)
            keyword_list.push(kk)
        }
    }
    console.log(keyword_list)
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
            // add string including processing
            const keyword = rp.dkanKeywordDatasetParser(parseData, keywords)
            // const keyword = rp.socrataKeywordDatasetParser(parseData, keywords)

            total_dataset += keyword.length
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
                /**
                 * check duplication
                 */
                // let check_dataset_duplicate = get.dataset_identifier_get(_dataset)
                // if (check_dataset_duplicate != undefined) { 
                //     // console.log(check_dataset_duplicate)
                //     check_dup_cnt++
                //     _dataset.id = check_dataset_duplicate.id
                // }
                // console.log(_dataset)

                // await create.dataset_create(_dataset)
                // await create.dataset_keyword_create(_datasetKeyword)
                if (_distribution.urls.length == undefined) {
                    resoucreUrls.push(Object.values(_distribution))
                } else {
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
            total_distribution += resoucreUrls.length
            // const rList = await rp.socrataKeywordToDatabaseParser(resoucreUrls)
            // console.log(rList)
            let format = 'csv'
            const rList = await rp.dkanKeywordFormatDistributionParser(resoucreUrls, format)
            // console.log(rList.length)
            console.log(`keyword based distribution parsing (it's final distribution counts): ${rList.length}`)
            for (let a = 0; a < rList.length; a++) {
                let dist = rList[a]
                if (dist.mediatype != null) {
                    dist.id = v1()
                    let check = dist.format
                    dist.format = check.toString().toUpperCase()
                    dist.publisher = sourceInfo.realPublisher
                    // await create.distribution_create(dist)
                }
                // console.log(dist)
            }

            if (rList) {
                const count = rList.length
                console.log(`Number of datasets in whole socrata catalog: ${dataset_count}`)
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
    console.log(`total dataset count: ${total_dataset}`)
    console.log(`total original distribution count: ${total_distribution}`)
    console.log(`total ${urlInfo.type} files in ${sourceInfo.publisher} open data portal: ${total_count}`)
    // console.log(`check dataset duplication count: ${check_dup_cnt}`)
    return total_count
}

async function socrataFormatBasedLogic(sourceInfo, keywords, format, end) {

    const dataDir = defaultPath
    let key_string = ""
    let keyword_list = []
    let getAll = await get.keyword_getAll()
    let f_get = await get.format_get(format)
    let dTheme = await get.theme_get('Health')
    let defaultTheme = {
        id: dTheme.getDataValue('id'),
        title: dTheme.getDataValue('title')
    }
    // sourceInfo.publisher = 
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
    /**
     * check existing keywords
     */
    let exist_keyword = []
    for (let a = 0; a < getAll.length; a++) {
        const check = getAll[a]['title']
        console.log(check)
        for (let k = 0; k < keywords.length; k++) {
            if (check == keywords[k]) {
                exist_keyword.push(keywords[k])
            }
        }
    }
    console.log(exist_keyword)
    for (let k = 0; k < keywords.length; k++) {
        if (exist_keyword.indexOf(keywords[k]) != -1) {
            let getKeyword = await get.keyword_get({ title: keywords[k] })
            let tmp_key = {
                id: getKeyword.getDataValue('id'),
                title: getKeyword.getDataValue('title')
            }
            keyword_list.push(tmp_key)
        } else {
            let kk = {
                id: v1(),
                title: keywords[k]
            }
            await create.keyword_create(kk)
            keyword_list.push(kk)
        }
    }
    console.log(keyword_list)
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
                console.log(_dataset)
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
                if (dist.mediatype != null) {
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

async function dkanFormatBasedLogic(sourceInfo, keywords, end) {

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

                // await create.dataset_create(_dataset)
                // await create.dataset_keyword_create(_datasetKeyword)
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
                if (dist.mediatype != null) {
                    dist.id = v1()
                    let check = dist.format
                    dist.format = check.toString().toUpperCase()
                    dist.publisher = sourceInfo.realPublisher
                    // await create.distribution_create(dist)
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

    // let tt = {
    //     id: v1(),
    //     title: "education"
    // }
    // await create.theme_create(tt)
    let total_distribution = 0
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
    // console.log(getAll)
    /**
     * check existing keywords
     */
    let exist_keyword = []
    for (let a = 0; a < getAll.length; a++) {
        const check = getAll[a]['title']
        console.log(check)
        for (let k = 0; k < keywords.length; k++) {
            if (check == keywords[k]) {
                exist_keyword.push(keywords[k])
            }
        }
    }
    // console.log(exist_keyword)
    for (let k = 0; k < keywords.length; k++) {
        if (exist_keyword.indexOf(keywords[k]) != -1) {
            let getKeyword = await get.keyword_get({ title: keywords[k] })
            let tmp_key = {
                id: getKeyword.getDataValue('id'),
                title: getKeyword.getDataValue('title')
            }
            keyword_list.push(tmp_key)
        } else {
            let kk = {
                id: v1(),
                title: keywords[k]
            }
            await create.keyword_create(kk)
            keyword_list.push(kk)
        }
    }
    // console.log(keyword_list)

    // console.log(`key_string: ${key_string}`)

    const urlInfo = {
        name: sourceInfo.name,
        type: 'domain',
        keywords: key_string,
        publisher: sourceInfo.publisher
    }
    let total_count = 0
    let total_dataset = 0
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
            // console.log(parseData)
            // const keyword = rp.socrataKeywordDatasetParser(parseData, keywords)
            const keyword = rp.dkanKeywordDatasetParser(parseData, keywords)
            // const keyword = await rp.keywordDatasetParserUpper(dataset, keywords)
            // console.log(keyword[0])
            let resoucreUrls = []
            console.log(`filtered datset counts: ${keyword.length}`)
            // total_dataset += keyword.length
            // total_dataset += d_count
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

                // console.log(_dataset)
                total_dataset++
                // await create.dataset_create(_dataset)
                // await create.dataset_keyword_create(_datasetKeyword)
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
            total_distribution += resoucreUrls.length
            const rList = await rp.socrataKeywordToDatabaseParser(resoucreUrls)
            // let format = 'csv'
            // const rList = await rp.dkanKeywordFormatDistributionParser(resoucreUrls, format)

            // console.log(rList.length)
            // console.log(rList)
            console.log(`keyword based distribution parsing results: ${rList.length}`)
            for (let a = 0; a < rList.length; a++) {
                let dist = rList[a]
                if (dist.mediatype != null) {
                    dist.id = v1()
                    let check = dist.format
                    dist.format = check.toString().toUpperCase()
                    dist.publisher = sourceInfo.realPublisher
                    // await create.distribution_create(dist)
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

                // const wUrls = await fh.writeDomainUrls(rList, urlInfo)
                // if (wUrls) {
                //     console.log(`write urls to files is succeeded`)
                // }
            }
        }
    }
    console.log(`total dataset : ${total_dataset}`)
    console.log(`total original distribution : ${total_distribution}`)
    console.log(`total ${urlInfo.type} files in ${sourceInfo.publisher} open data portal: ${total_count}`)
    return total_count
}

async function junarKeywordBasedLogic(sourceInfo, keywords, end) {

    const dataDir = defaultPath
    let key_string = ""
    let keyword_list = []
    let getAll = await get.keyword_getAll()

    // let tt = {
    //     id: v1(),
    //     title: "education"
    // }
    // await create.theme_create(tt)

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
    console.log(getAll)
    /**
     * check existing keywords
     */
    let exist_keyword = []
    for (let a = 0; a < getAll.length; a++) {
        const check = getAll[a]['title']
        console.log(check)
        for (let k = 0; k < keywords.length; k++) {
            if (check == keywords[k]) {
                exist_keyword.push(keywords[k])
            }
        }
    }
    console.log(exist_keyword)
    for (let k = 0; k < keywords.length; k++) {
        if (exist_keyword.indexOf(keywords[k]) != -1) {
            let getKeyword = await get.keyword_get({ title: keywords[k] })
            let tmp_key = {
                id: getKeyword.getDataValue('id'),
                title: getKeyword.getDataValue('title')
            }
            keyword_list.push(tmp_key)
        } else {
            let kk = {
                id: v1(),
                title: keywords[k]
            }
            await create.keyword_create(kk)
            keyword_list.push(kk)
        }
    }
    console.log(keyword_list)
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
            // console.log(parseData)
            const keyword = rp.socrataKeywordDatasetParser(parseData, keywords)
            // const keyword = await rp.keywordDatasetParserUpper(dataset, keywords)
            // console.log(keyword[0])
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
                console.log(_dataset)
                // await create.dataset_create(_dataset)
                // await create.dataset_keyword_create(_datasetKeyword)
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
            console.log(rList)
            console.log(`keyword based distribution parsing results: ${rList.length}`)
            for (let a = 0; a < rList.length; a++) {
                let dist = rList[a]
                if (dist.mediatype != null) {
                    dist.id = v1()
                    let check = dist.format
                    dist.format = check.toString().toUpperCase()
                    dist.publisher = sourceInfo.realPublisher
                    // await create.distribution_create(dist)
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
    let total_dataset = 0
    let total_distribution = 0

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
    let exist_keyword = []
    for (let a = 0; a < getAll.length; a++) {
        const check = getAll[a]['title']
        console.log(check)
        for (let k = 0; k < keywords.length; k++) {
            if (check == keywords[k]) {
                exist_keyword.push(keywords[k])
            }
        }
    }
    console.log(exist_keyword)
    for (let k = 0; k < keywords.length; k++) {
        if (exist_keyword.indexOf(keywords[k]) != -1) {
            let getKeyword = await get.keyword_get({ title: keywords[k] })
            let tmp_key = {
                id: getKeyword.getDataValue('id'),
                title: getKeyword.getDataValue('title')
            }
            keyword_list.push(tmp_key)
        } else {
            let kk = {
                id: v1(),
                title: keywords[k]
            }
            await create.keyword_create(kk)
            keyword_list.push(kk)
        }
    }
    // console.log(keyword_list)
    // console.log(`key_string: ${key_string}`)

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
            // const keyword = rp.socrataKeywordDatasetParser(parseData, keywords)
            const keyword = rp.dkanKeywordDatasetParser(parseData, keywords)

            let d_count = keyword.length
            let resoucreUrls = []
            total_dataset = d_count
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
                console.log(_dataset)
                // await create.dataset_create(_dataset)
                // await create.dataset_keyword_create(_datasetKeyword)
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
            total_distribution += resoucreUrls.length
            // const rList = await rp.arcgisKeywordToDatabaseParser(resoucreUrls)
            let format = 'csv'
            const rList = await rp.arcgisKeywordForamtDistributionParser(resoucreUrls, format)

            // console.log(rList.length)
            console.log(`keyword based distribution parsing results: ${rList.length}`)
            for (let a = 0; a < rList.length; a++) {
                let dist = rList[a]
                if (dist.mediatype != null) {
                    dist.id = v1()
                    let check = dist.format
                    dist.format = check.toString().toUpperCase()
                    dist.publisher = sourceInfo.realPublisher
                    // await create.distribution_create(dist)
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

                // const wUrls = await fh.writeDomainUrls(rList, urlInfo)
                // if (wUrls) {
                //     console.log(`write urls to files is succeeded`)
                // }
            }
        }
    }
    // }
    console.log(`total dataset count: ${total_dataset}`)
    console.log(`total original distribution count: ${total_distribution}`)
    console.log(`total ${urlInfo.type} files in ${sourceInfo.publisher} open data portal: ${total_count}`)
    return total_count
}

async function arcgisFormatBasedLogic(sourceInfo, keywords, format, end) {

    const dataDir = defaultPath
    let key_string = ""
    let keyword_list = []
    let getAll = await get.keyword_getAll()
    let total_dataset = 0
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
    let exist_keyword = []
    for (let a = 0; a < getAll.length; a++) {
        const check = getAll[a]['title']
        console.log(check)
        for (let k = 0; k < keywords.length; k++) {
            if (check == keywords[k]) {
                exist_keyword.push(keywords[k])
            }
        }
    }
    console.log(exist_keyword)
    for (let k = 0; k < keywords.length; k++) {
        if (exist_keyword.indexOf(keywords[k]) != -1) {
            let getKeyword = await get.keyword_get({ title: keywords[k] })
            let tmp_key = {
                id: getKeyword.getDataValue('id'),
                title: getKeyword.getDataValue('title')
            }
            keyword_list.push(tmp_key)
        } else {
            let kk = {
                id: v1(),
                title: keywords[k]
            }
            await create.keyword_create(kk)
            keyword_list.push(kk)
        }
    }
    console.log(keyword_list)
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
            // const keyword = rp.socrataKeywordDatasetParser(parseData, keywords)
            const keyword = rp.dkanKeywordDatasetParser(parseData, keywords)

            let d_count = keyword.length
            let resoucreUrls = []
            total_dataset = d_count
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
                console.log(_dataset)
                // await create.dataset_create(_dataset)
                // await create.dataset_keyword_create(_datasetKeyword)
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
            // let format = 'csv' lowercase
            const rList = await rp.arcgisKeywordForamtDistributionParser(resoucreUrls, format)

            // console.log(rList.length)
            console.log(`keyword based distribution parsing results: ${rList.length}`)
            for (let a = 0; a < rList.length; a++) {
                let dist = rList[a]
                if (dist.mediatype != null) {
                    dist.id = v1()
                    let check = dist.format
                    dist.format = check.toString().toUpperCase()
                    dist.publisher = sourceInfo.realPublisher
                    // await create.distribution_create(dist)
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
    console.log(`total dataset count: ${total_dataset}`)
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
    let exist_keyword = []
    for (let a = 0; a < getAll.length; a++) {
        const check = getAll[a]['title']
        // console.log(check)
        for (let k = 0; k < keywords.length; k++) {
            if (check == keywords[k]) {
                exist_keyword.push(keywords[k])
            }
        }
    }
    // console.log(exist_keyword)
    for (let k = 0; k < keywords.length; k++) {
        if (exist_keyword.indexOf(keywords[k]) != -1) {
            let getKeyword = await get.keyword_get({ title: keywords[k] })
            let tmp_key = {
                id: getKeyword.getDataValue('id'),
                title: getKeyword.getDataValue('title')
            }
            keyword_list.push(tmp_key)
        } else {
            let kk = {
                id: v1(),
                title: keywords[k]
            }
            await create.keyword_create(kk)
            keyword_list.push(kk)
        }
    }

    // console.log(`key_string: ${key_string}`)

    const urlInfo = {
        name: sourceInfo.name,
        type: 'domain',
        keywords: key_string,
        publisher: sourceInfo.publisher
    }
    let total_count = 0
    let total_dataset = 0
    for (let page = 1; page <= end; page++) {
        sourceInfo.page = page
        const catalog = await fh.readCatalog(dataDir, sourceInfo)
        // console.log(catalog)
        if (!catalog) {
            console.log(`read data is failed`)
        } else {
            // const parseData = JSON.parse(catalog)
            const parseData = await rp.catalogParser(catalog)
            // console.log(parseData)
            const dataset = await rp.odsKeyFormatDatasetParser(parseData, keywords)
            // const dataset = parseData.dataset
            console.log(dataset)
            // const dataset_count = dataset.length
            // const keyword = rp.odsKeywordDatasetParser(dataset, keywords)
            // console.log(keyword)
            let resoucreUrls = []
            // console.log(keyword.length)
            total_dataset += keyword.length
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

                // await create.dataset_create(_dataset)
                // await create.dataset_keyword_create(_datasetKeyword)
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
                if (dist.url != undefined) {
                    dist.id = v1()
                    let check = dist.format
                    dist.format = check.toString().toUpperCase()
                    if (dist.mediatype == undefined) {
                        dist.mediatype = mime.lookup(check)
                    }
                    dist.publisher = sourceInfo.realPublisher
                    // console.log(dist)
                    rdist.push(dist)
                    // await create.distribution_create(dist)
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

                // const wUrls = await fh.writeDomainUrls(rdist, urlInfo)
                // if (wUrls) {
                //     console.log(`write urls to files is succeeded`)
                // }
            }
        }
    }
    console.log(`total dataset counts: ${total_dataset}`)
    console.log(`total ${urlInfo.type} files in ${sourceInfo.publisher} open data portal: ${total_count}`)
    return total_count
}

// async function odsKeywordBasedLogic(sourceInfo, keywords, end) {

//     const dataDir = defaultPath
//     let key_string = ""
//     let keyword_list = []
//     let getAll = await get.keyword_getAll()
//     // sourceInfo.publisher = 
//     let total_dataset = 0
//     let total_distribution = 0
//     for (let k = 0; k < keywords.length; k++) {
//         if (k == 0) {
//             let string = keywords[k]
//             key_string = string.replace(/-/g, '')
//         }
//         else {
//             let string = keywords[k]
//             key_string = key_string + '-' + string.replace(/-/g, '')
//         }
//     }
//     /**
//      * check existing keywords
//      */
//     let exist_keyword = []
//     for (let a = 0; a < getAll.length; a++) {
//         const check = getAll[a]['title']
//         console.log(check)
//         for (let k = 0; k < keywords.length; k++){
//             if (check == keywords[k]) {
//                 exist_keyword.push(keywords[k])
//             }
//         }
//     }
//     console.log(exist_keyword)
//     for (let k = 0; k < keywords.length; k++) {
//         if (exist_keyword.indexOf(keywords[k]) != -1){
//             let getKeyword = await get.keyword_get({ title: keywords[k] })
//             let tmp_key = {
//                 id: getKeyword.getDataValue('id'),
//                 title: getKeyword.getDataValue('title')
//             }
//             keyword_list.push(tmp_key)
//         } else {
//             let kk = {
//                 id: v1(),
//                 title: keywords[k]
//             }
//             await create.keyword_create(kk)
//             keyword_list.push(kk)
//         }
//     }
//     console.log(keyword_list)
//     console.log(`key_string: ${key_string}`)

//     const urlInfo = {
//         name: sourceInfo.name,
//         type: 'domain',
//         keywords: key_string,
//         publisher: sourceInfo.publisher
//     }
//     let total_count = 0
//     for (let page = 1; page <= end; page++) {
//         sourceInfo.page = page
//         const catalog = await fh.readCatalogJson(dataDir, sourceInfo)
//         // console.log(catalog)
//         if (!catalog) {
//             console.log(`read data is failed`)
//         } else {
//             const parseData = JSON.parse(catalog)
//             console.log(parseData)
//             const dataset = parseData.dataset

//             const dataset_count = dataset.length
//             // add string including processing
//             const keyword =  rp.dkanKeywordDatasetParser(parseData, keywords)
//             // const keyword = rp.socrataKeywordDatasetParser(parseData, keywords)

//             total_dataset += keyword.length
//             let resoucreUrls = []
//             console.log(keyword.length)
//             for (let t = 0; t < keyword.length; t++) {

//                 let temp = keyword[t]
//                 const _dataset = temp.dataset
//                 _dataset.id = v1()
//                 _dataset.publisher = sourceInfo.realPublisher
//                 let _distribution = {
//                     urls: temp.distribution,
//                     dataset_id: _dataset.id,
//                     title: _dataset.title,
//                     identifier: _dataset.identifier
//                 }
//                 let key_id
//                 for (let k = 0; k < keyword_list.length; k++) {
//                     if (keyword_list[k]['title'] == _dataset.keyword) {
//                         key_id = keyword_list[k]['id']
//                     }
//                 }
//                 let _datasetKeyword = {
//                     id: v1(),
//                     dataset_id: _dataset.id,
//                     keyword_id: key_id,
//                     state: 'active'
//                 }
//                 // console.log(_dataset)
//                 // await create.dataset_create(_dataset)
//                 // await create.dataset_keyword_create(_datasetKeyword)
//                 if (_distribution.urls.length == undefined) {
//                     resoucreUrls.push(Object.values(_distribution))
//                 }
//                 else {
//                     for (let w = 0; w < _distribution.urls.length; w++) {
//                         let k = {
//                             url: _distribution.urls[w],
//                             dataset_id: _distribution.dataset_id,
//                             title: _distribution.title,
//                             identifier: _distribution.identifier
//                         }
//                         resoucreUrls.push(Object.values(k))
//                     }
//                 }
//             }

//             const rList = await rp.socrataKeywordToDatabaseParser(resoucreUrls)
//             // let format = 'csv'
//             // const rList = await rp.dkanKeywordFormatDistributionParser(resoucreUrls, format)
//             // console.log(rList.length)
//             console.log(`keyword based distribution parsing (it's final distribution counts): ${rList.length}`)
//             for (let a = 0; a < rList.length; a++) {
//                 let dist = rList[a]
//                 if (dist.mediatype != null) {
//                     dist.id = v1()
//                     let check = dist.format
//                     dist.format = check.toString().toUpperCase()
//                     dist.publisher = sourceInfo.realPublisher
//                     // await create.distribution_create(dist)
//                 }
//                 // console.log(dist)
//             }

//             if (rList) {
//                 const count = rList.length
//                 console.log(`Number of datasets in whole socrata catalog: ${dataset_count}`)
//                 console.log(`number of ${keywords} files in socrata catalog: ${count}`)
//                 urlInfo.count = count
//                 urlInfo.page = page
//                 total_count = count

//                 // const wUrls = await fh.writeDomainUrls(rList, urlInfo)
//                 // if (wUrls) {
//                 //     console.log(`write urls to files is succeeded`)
//                 // }
//             }
//         }
//     }
//     console.log(`total dataset count: ${total_dataset}`)
//     console.log(`total distribution count: ${total_distribution}`)
//     console.log(`total ${urlInfo.type} files in ${sourceInfo.publisher} open data portal: ${total_count}`)
//     return total_count
// }

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