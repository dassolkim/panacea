const { XMLParser, XMLBuilder, XMLValidator } = require('fast-xml-parser')
const uuid = require('uuid')
const mime = require('mime-types')
module.exports = {
    catalogParser, distributionParser, formatDistributionParser, keywordParser, keywordDistributionParser,
    datasetParser, schemaParser, socrataDatasetParser, odsDatasetParser, keywordDatasetParser, keywordDistToDatabaseParser,
    socrataKeywordDatasetParser, socrataKeywordToDatabaseParser, arcgisKeywordToDatabaseParser, odsKeywordDatasetParser, odsKeywordToDatabaseParser, formatDistToDatabaseParser,
    keywordDatasetParserUpper, dkanKeywordDatasetParser, dkanKeywordFormatDistributionParser, arcgisKeywordForamtDistributionParser,
    odsKeyFormatDatasetParser

}


function catalogParser(data) {
    const options = {
        ignoreAttributes: false,
        attributeNamePrefix: "@_"
    }
    const catalogParser = new XMLParser(options);
    const catalog = catalogParser.parse(data);

    return catalog
}

function socrataCatalogParser(data) {
    const dataset = data.dataset
    return dataset
}

function distributionParser(catalog, format) {

    const data = catalog
    const dist = data['rdf:RDF']['dcat:Distribution'] // US & UK catalog, DKAN (Oklahoma)
    // const dist = data['rdf:RDF']['rdf:Description']  // CA catalog

    if (dist) {
        const count = Object.keys(dist).length;
        console.log(`Number of Distribution in Catalog: ${count}`)
        let i = 0
        let j = 0
        let url_list = []
        while (i < count) {
            const exist = dist[i]
            if (exist != undefined) {
                if (dist[i]['dct:format'] == format) {
                    if (dist[i]['dcat:accessURL']) {
                        url_list[j] = dist[i]['dcat:accessURL']['@_rdf:resource']
                        j++
                    }
                }
                i++
            } else { i++ }

        }
        return url_list
    } else { return false }
    //console.log(dist)
}

function formatDistributionParser(catalog, format) {

    const data = catalog
    const dist = data['rdf:RDF']['dcat:Distribution'] // US & UK catalog, DKAN (Oklahoma)
    // const dist = data['rdf:RDF']['rdf:Description']  // CA catalog

    if (dist) {
        const count = Object.keys(dist).length;
        console.log(`Number of Distribution in Catalog: ${count}`)
        let i = 0
        let j = 0
        let url_list = []
        while (i < count) {
            const exist = dist[i]
            if (exist != undefined) {
                if (dist[i]['dct:format'] == format) {
                    if (dist[i]['dcat:accessURL']) {
                        let temp = {
                            title: dist[i]['dct:title'],
                            url: dist[i]['dcat:accessURL']['@_rdf:resource'],
                            format: dist[i]['dct:format'],
                            mediatype: dist[i]['dcat:mediaType'],
                            identifier: dist[i]['@_rdf:about']
                        }
                        // url_list[j] = dist[i]['dct:title']
                        // url_list[j].push(dist[i]['dcat:accessURL']['@_rdf:resource'])
                        url_list[j] = temp
                        j++
                    }
                }
                i++
            } else { i++ }
        }
        return url_list
    } else { return false }
    //console.log(dist)
}
/* 
Latest Dataset Paser based on Keyword logic
Pancea
*/
function keywordDatasetParser(dataset, keywords) {

    const count = dataset.length
    let i = 0
    const results = []
    while (i < count) {
        const exist = dataset[i]
        if (exist != undefined) {
            let tag = dataset[i]['dcat:Dataset']['dcat:keyword']
            // console.log(tag)
            // if(tag.indexOf(keywords) != -1)
            let j = 0
            while (j < keywords.length) {
                // console.log(tag)
                // let up_tag = tag[j].toUpperCase()
                if (tag != undefined && tag.indexOf(keywords[j]) != -1) {
                    let temp = {
                        dataset: {
                            id: "string",
                            url: dataset[i]['dcat:Dataset']['@_rdf:about'],
                            title: dataset[i]['dcat:Dataset']['dct:title'],
                            issued: dataset[i]['dcat:Dataset']['dct:issued']['#text'],
                            modified: dataset[i]['dcat:Dataset']['dct:modified']['#text'],
                            identifier: dataset[i]['dcat:Dataset']['dct:identifier']['@_rdf:resource'],
                            theme: dataset[i]['dcat:Dataset']['dcat:theme'], // with theme
                            // keyword: keywords[j]['dcat:theme']['@_rdf:resource']
                            keyword: keywords[j]
                        },
                        distribution: dataset[i]['dcat:Dataset']['dcat:distribution']
                    }
                    results.push(temp)
                }
                j++
            }
        }
        i++
    }
    return results
}

function keywordDatasetParserUpper(dataset, keywords) {

    const count = dataset.length
    let i = 0
    const results = []
    while (i < count) {
        const exist = dataset[i]
        if (exist != undefined) {
            let tag = dataset[i]['dcat:Dataset']['dcat:keyword']
            // console.log(tag)
            // if(tag.indexOf(keywords) != -1)
            let j = 0
            // if (i == 200) {break}
            while (j < keywords.length) {
                // console.log(tag)
                // let up_tag = tag[j].toUpperCase()

                if (tag != undefined) {
                    for (let k = 0; k < tag.length; k++) {
                        let key_input = keywords[j]
                        let tag_temp = tag[k]
                        if (typeof (tag_temp) == 'string') { // if (typeof(t_temp) == 'string' && temp.toUpperCase() == t_temp.toUpperCase()) {
                            let upper = key_input.toUpperCase()
                            let original_tag = tag_temp.toUpperCase()
                            let check_title = dataset[i]['dcat:Dataset']['dct:title']
                            let title = dataset[i]['dcat:Dataset']['dct:title']
                            if(check_title != undefined && check_title.length > 250){
                                title = check_title.substr(0, 240)
                            }
                            // let description = dataset[i]['dcat:Dataset']['dct:title']
                            if (original_tag.indexOf(upper) != -1) {
                                let temp = {
                                    dataset: {
                                        id: "string",
                                        url: dataset[i]['dcat:Dataset']['@_rdf:about'],
                                        // title: dataset[i]['dcat:Dataset']['dct:title'],
                                        title: title,
                                        issued: dataset[i]['dcat:Dataset']['dct:issued']['#text'],
                                        description: dataset[i]['dcat:Dataset']['dct:title'],
                                        modified: dataset[i]['dcat:Dataset']['dct:modified']['#text'],
                                        identifier: dataset[i]['dcat:Dataset']['dct:identifier']['@_rdf:resource'],
                                        theme: dataset[i]['dcat:Dataset']['dcat:theme'], // with theme
                                        // keyword: keywords[j]['dcat:theme']['@_rdf:resource']
                                        keyword: keywords[j]
                                    },
                                    distribution: dataset[i]['dcat:Dataset']['dcat:distribution']
                                }
                                results.push(temp)
                            }
                        }
                    }
                }
                j++
            }
        }
        i++
    }
    return results
}

function keywordParser(dataset, keywords) {

    const count = dataset.length
    let i = 0
    const results = []
    while (i < count) {
        const exist = dataset[i]
        if (exist != undefined) {
            let tag = dataset[i]['dcat:Dataset']['dcat:keyword']
            // if(tag.indexOf(keywords) != -1)
            let j = 0
            while (j < keywords.length) {
                if (tag.indexOf(keywords[j]) != -1) {
                    let temp = {
                        dataset: dataset[i]['dcat:Dataset']['@_rdf:about'],
                        distribution: dataset[i]['dcat:Dataset']['dcat:distribution']
                    }
                    results.push(temp)
                }
                j++
            }
        }
        i++
    }
    // console.log(results)
    return results
}

function keywordDistributionParser(catalog, resoucreUrls) {

    const data = catalog
    // const dist = data['rdf:RDF']['dcat:Distribution'] // US & UK catalog, DKAN (Oklahoma)
    const dist = data['rdf:RDF']['rdf:Description']  // CA catalog, Opendatasoft
    const rcount = resoucreUrls.length
    if (dist) {
        const count = Object.keys(dist).length;
        console.log(`(rdf_parser) # of Distributions in Catalog: ${count}`)
        let i = 0
        let j = 0
        let url_list = []
        while (i < count) {
            const exist = dist[i]
            // if (i == 1) console.log(exist)
            if (exist != undefined) {
                if (dist[i]['dcat:accessURL']) {
                    // if (i % 2 == 0) console.log(dist[i])
                    if (resoucreUrls.indexOf(dist[i]['@_rdf:about']) != -1) {
                        let temp = {
                            url: dist[i]['dcat:accessURL']['@_rdf:resource'],
                            format: dist[i]['dct:format'],
                            mediatype: dist[i]['dcat:mediaType'],
                            identifier: dist[i]['@_rdf:about']
                        }
                        // if(i % 10 == 0) console.log(dist[i])
                        url_list.push(temp)
                    }
                    i++
                }
            } else { i++ }
        }
        // console.log(url_list)
        // console.log(url_list.length)
        return url_list
    } else { return false }
    //console.log(dist)
}

/* 
Latest Function for conrtoller_main logic
Panacea
*/
function keywordDistToDatabaseParser(catalog, resoucreUrls) {

    const data = catalog
    const dist = data['rdf:RDF']['dcat:Distribution'] // US & UK catalog, DKAN (Oklahoma)
    // const dist = data['rdf:RDF']['rdf:Description']  // CA catalog
    const rcount = resoucreUrls.length
    console.log(`(rdf_parser) # of resourceUrls: ${rcount}`)
    let filtered_cnt = 0
    if (dist) {
        const count = Object.keys(dist).length;
        console.log(`(rdf_parser) # of Distribution in Catalog: ${count}`)
        let i = 0
        let j = 0
        let url_list = []

        while (i < count) {
            const exist = dist[i]
            let fc_cnt = 0
            // if (i == 1) console.log(exist)
            if (i == 200) { break }
            if (exist != undefined) {
                if (dist[i]['dcat:accessURL'] != undefined) {
                    // if (i % 2 == 0) console.log(dist[i])
                    // if(dist[i]['dct:format'] == undefined) {continue}
                    for (let k = 0; k < rcount; k++) {
                        if (dist[i]['dct:format'] == undefined) { 
                            fc_cnt++
                            continue
                        }
                        let mediaType
                        let mediatype = dist[i]['dcat:mediaType']
                        if (mediatype == undefined) { mediaType = dist[i]['dct:format'] } else { mediaType = dist[i]['dcat:mediaType'] }
                        if (resoucreUrls[k][0]['@_rdf:resource'] == dist[i]['@_rdf:about']) {
                            let temp = {
                                url: dist[i]['dcat:accessURL']['@_rdf:resource'],
                                format: dist[i]['dct:format'],
                                mediatype: mediaType,
                                identifier: dist[i]['@_rdf:about'],
                                dataset_id: resoucreUrls[k][1],
                                title: resoucreUrls[k][2]
                            }
                            // if(i % 10 == 0) console.log(dist[i])
                            url_list.push(temp)
                            // console.log(temp)
                        }
                    }
                }
            }
            i++
            filtered_cnt += fc_cnt
        }
        console.log(`(rdf_parser) # of Filtered Distribution in Catalog: ${filtered_cnt}`)
        return url_list
    } else {
        return false
    }
}

function formatDistToDatabaseParser(catalog, format, resoucreUrls) {

    const data = catalog
    const dist = data['rdf:RDF']['dcat:Distribution'] // US & UK catalog, DKAN (Oklahoma)
    // const dist = data['rdf:RDF']['rdf:Description']  // CA catalog
    const rcount = resoucreUrls.length
    // console.log(dist)
    // console.log(resoucreUrls.length)
    console.log(`(rdf_parser) # of resourceUrls: ${rcount}`)
    // console.log(dist)
    if (dist) {
        const count = Object.keys(dist).length;
        console.log(`(rdf_parser) # of Distribution in Catalog: ${count}`)
        let i = 0
        let j = 0
        let url_list = []
        while (i < count) {
            const exist = dist[i]
            // if (i == 1) console.log(exist)
            if (i == 200) break
            if (exist != undefined && resoucreUrls.length != 0) {
                if (dist[i]['dcat:accessURL']) {
                    // if (i % 2 == 0) console.log(dist[i])
                    for (let k = 0; k < rcount; k++) {
                        let mediaType
                        let mediatype = dist[i]['dcat:mediaType']
                        if (mediatype == undefined) { mediaType = dist[i]['dct:format'] } else { mediaType = dist[i]['dcat:mediaType'] }
                        if (resoucreUrls[k][0]['@_rdf:resource'] == dist[i]['@_rdf:about']) {
                            // console.log(`This is print out formatDistToDatabaseParser resourceUrls: ${dist[i]}`)
                            
                            if (dist[i]['dct:format'] == format) {
                                let temp = {
                                    url: dist[i]['dcat:accessURL']['@_rdf:resource'],
                                    format: dist[i]['dct:format'],
                                    mediatype: mediaType,
                                    identifier: dist[i]['@_rdf:about'],
                                    dataset_id: resoucreUrls[k][1],
                                    title: resoucreUrls[k][2]
                                }
                                // if(i % 10 == 0) console.log(dist[i])
                                url_list.push(temp)
                                // console.log(temp)
                                // console.log(`This is print out formatDistToDatabaseParser resourceUrls: ${url_list}`)
                            }
                        }
                    }
                }
                i++
            } else { i++ }

        }
        console.log(url_list)
        // console.log(url_list.length)
        return url_list
    } else { return false }
    //console.log(dist)
}

function formatDistMultiParser(catalog, formats, resoucreUrls) {

    const data = catalog
    const dist = data['rdf:RDF']['dcat:Distribution'] // US & UK catalog, DKAN (Oklahoma)
    // const dist = data['rdf:RDF']['rdf:Description']  // CA catalog
    const rcount = resoucreUrls.length
    // console.log(dist)
    // console.log(resoucreUrls.length)
    console.log(`(rdf_parser) # of resourceUrls: ${rcount}`)
    // console.log(dist)
    if (dist) {
        const count = Object.keys(dist).length;
        console.log(`(rdf_parser) # of Distribution in Catalog: ${count}`)
        let i = 0
        let j = 0
        let url_list = []
        while (i < count) {
            const exist = dist[i]
            // if (i == 1) console.log(exist)
            if (i == 200) break
            if (exist != undefined && resoucreUrls.length != 0) {
                if (dist[i]['dcat:accessURL']) {
                    // if (i % 2 == 0) console.log(dist[i])
                    for (let k = 0; k < rcount; k++) {
                        let mediaType
                        let mediatype = dist[i]['dcat:mediaType']
                        if (mediatype == undefined) { mediaType = dist[i]['dct:format'] } else { mediaType = dist[i]['dcat:mediaType'] }
                        if (resoucreUrls[k][0]['@_rdf:resource'] == dist[i]['@_rdf:about']) {
                            // console.log(`This is print out formatDistToDatabaseParser resourceUrls: ${dist[i]}`)
                            for (let f = 0; f < formats.length; f++) {
                                if (dist[i]['dct:format'] == formats[f]) {
                                    let temp = {
                                        url: dist[i]['dcat:accessURL']['@_rdf:resource'],
                                        format: dist[i]['dct:format'],
                                        mediatype: mediaType,
                                        identifier: dist[i]['@_rdf:about'],
                                        dataset_id: resoucreUrls[k][1],
                                        title: resoucreUrls[k][2]
                                    }
                                    // if(i % 10 == 0) console.log(dist[i])
                                    url_list.push(temp)
                                    // console.log(temp)
                                    // console.log(`This is print out formatDistToDatabaseParser resourceUrls: ${url_list}`)
                                }
                            }
                        }
                    }
                }
                i++
            } else { i++ }

        }
        console.log(url_list)
        // console.log(url_list.length)
        return url_list
    } else { return false }
    //console.log(dist)
}

function datasetParser(catalog) {

    //console.log(data)
    const dataset = catalog['rdf:RDF']['dcat:Catalog']['dcat:dataset'] // US & UK catalog, DKAN (Oklahoma)

    // const dataset = catalog['rdf:RDF']['rdf:Description'][0]['dcat:dataset']  // CA catalog
    // const dataset = catalog['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dcat:distribution']
    // console.log(dataset)

    const count = dataset.length
    console.log(`Number of Datasets in Catalog: ${count}`)

    return dataset
}


function schemaParser(catalog) {
    const schema = catalog['rdf:RDF']['hydra:PagedCollection']

    const fp = schema['hydra:firstPage']
    const lp = schema['hydra:lastPage']
    const np = schema['hydra:nextPage']
    const ti = schema['hydra:totalItems']['#text']

    const results = {
        firstPage: fp,
        lastPage: lp,
        nextPage: np,
        totalItem: ti
    }

    return results
}

function socrataDatasetParser(catalog) {

    const dataset = catalog.dataset
    const dataset_count = dataset.length
    // console.log(dataset_count)
    let dist_list = []

    for (let nd = 0; nd < dataset_count; nd++) {
        dist_list[nd] = dataset[nd]['distribution']
    }

    return dist_list
}

function socrataKeywordDatasetParser(catalog, keywords) {

    // const dataset = catalog.dataset
    const dataset = catalog.dataset
    const count = dataset.length
    const kcount = keywords.length
    const results = []
    // console.log(count)
    for (let i = 0; i < count; i++) {
        let exist = dataset[i]
        // if(i % 10 == 0) console.log(exist)
        if (exist != undefined) {
            let tag = []
            tag = dataset[i]['keyword']
            // console.log(tag)
            // if(i % 100 == 0) console.log(tag)

            let _issued = dataset[i]['issued']
            let _url = dataset[i]['landingPage']
            if (_issued == undefined) {
                _issued = dataset[i]['modified']
            }
            if (_url == undefined) {
                _url = dataset[i]['identifier']
            }
            for (let j = 0; j < kcount; j++) {
                if (tag != undefined) {
                    if (tag.indexOf(keywords[j]) != -1) {
                        let temp = {
                            dataset: {
                                id: "string",
                                // url: dataset[i]['landingPage'],
                                url: _url,
                                title: dataset[i]['title'],
                                // issued: dataset[i]['issued'],
                                issued: _issued,
                                modified: dataset[i]['modified'],
                                identifier: dataset[i]['identifier'],
                                keyword: keywords[j]
                            },
                            distribution: dataset[i]['distribution']
                        }
                        results.push(temp)
                        // if (i == 1) console.log(`print in rdf-parser: ${temp}`)
                    }
                }
            }
        }
    }
    return results
}
/* 
Latest Function for conrtoller_main logic (DKAN, Socrata)
Panacea
*/
function dkanKeywordDatasetParser(catalog, keywords) {

    // const dataset = catalog.dataset
    const ds = catalog.dataset
    const count = ds.length
    const kcount = keywords.length
    const results = []
    console.log(count)
    for (let i = 0; i < count; i++) {
        let exist = ds[i]
        // if(i % 10 == 0) console.log(exist)
        if (exist != undefined) {
            let tag = ds[i]['keyword']
            if (i == 0) console.log(tag)
            let _issued = ds[i]['issued']
            let _url = ds[i]['landingPage']
            if (_issued == undefined) {
                _issued = ds[i]['modified']
            } else if (_url == undefined) {
                _url = ds[i]['identifier']
            }
            if (tag != undefined) {
                let tcount = tag.length
                for (let k = 0; k < tcount; k++) {
                    let tag_temp = tag[k]
                    for (let j = 0; j < kcount; j++) {
                        let key_input = keywords[j]
                        if (typeof(tag_temp) == 'string') {
                            let upper = key_input.toUpperCase()
                            let original_tag = tag_temp.toUpperCase()
                            if (original_tag.indexOf(upper) != -1) {
                                let temp = {
                                    dataset: {
                                        id: "string",
                                        // url: dataset[i]['landingPage'],
                                        url: _url,
                                        title: ds[i]['title'],
                                        // issued: dataset[i]['issued'],
                                        issued: _issued,
                                        modified: ds[i]['modified'],
                                        identifier: ds[i]['identifier'],
                                        keyword: keywords[j]
                                    },
                                    distribution: ds[i]['distribution']
                                }
                                results.push(temp)
                                // if (j == 1) {console.log(`print in rdf-parser: ${temp}`)}
                            }
                        }
                    }
                }
            }
        }
    }
    console.log(results.length)
    return results
}

function dkanKeywordFormatDistributionParser(filteredData, format) {


    const fcount = filteredData.length

    const url_list = []

    for (let i = 0; i < fcount; i++) {
        let rdist = filteredData[i]
        if (rdist != undefined) {
            let t_mime = rdist[0]['mediaType']
            let t_format = mime.extension(t_mime)
            // console.log(t_format)
            if (t_format == format) {
                let temp = {
                    url: rdist[0]['downloadURL'],
                    format: t_format,
                    mediatype: t_mime,
                    identifier: rdist[3],
                    dataset_id: rdist[1],
                    title: rdist[2]
                }
                // if(i % 10 == 0) console.log(dist[i])
                url_list.push(temp)
                // if(i % 10== 0) console.log(`print in rdf-parser: ${temp}`)
            }
        }
    }
    return url_list
}


function socrataKeywordToDatabaseParser(filteredData) {


    const fcount = filteredData.length
    // const kcount = keywords.length
    const url_list = []
    // console.log(count)
    // let format = mime.extension(dist.format)

    for (let i = 0; i < fcount; i++) {
        let rdist = filteredData[i]
        if (rdist != undefined) {
            let temp = {
                url: rdist[0]['downloadURL'],
                format: mime.extension(rdist[0]['mediaType']),
                mediatype: rdist[0]['mediaType'],
                identifier: rdist[3],
                dataset_id: rdist[1],
                title: rdist[2]
            }
            // if(i % 10 == 0) console.log(dist[i])
            url_list.push(temp)
            // if(i % 10== 0) console.log(`print in rdf-parser: ${temp}`)
        }
    }
    return url_list
}

function arcgisKeywordToDatabaseParser(filteredData) {

    const fcount = filteredData.length
    // const kcount = keywords.length
    const url_list = []
    // console.log(count)
    // let format = mime.extension(dist.format)

    for (let i = 0; i < fcount; i++) {
        let rdist = filteredData[i]
        if (rdist != undefined) {
            let temp = {
                url: rdist[0]['accessURL'],
                format: mime.extension(rdist[0]['mediaType']),
                mediatype: rdist[0]['mediaType'],
                identifier: rdist[3],
                dataset_id: rdist[1],
                title: rdist[2]
            }
            // if(i % 10 == 0) console.log(dist[i])
            url_list.push(temp)
            // if(i % 10== 0) console.log(`print in rdf-parser: ${temp}`)
        }
    }
    return url_list
}

function arcgisKeywordForamtDistributionParser(filteredData, format) {

    const fcount = filteredData.length
    // const kcount = keywords.length
    const url_list = []
    // console.log(count)
    // let format = mime.extension(dist.format)

    for (let i = 0; i < fcount; i++) {
        let rdist = filteredData[i]
        if (rdist != undefined) {
            let t_mime = rdist[0]['mediaType']
            let t_format = mime.extension(t_mime)
            // console.log(t_format)
            if (t_format == format) {
                let temp = {
                    url: rdist[0]['accessURL'],
                    format: t_format,
                    mediatype: t_mime,
                    identifier: rdist[3],
                    dataset_id: rdist[1],
                    title: rdist[2]
                }
                url_list.push(temp)
            }
        }
    }
    return url_list
}

function odsKeywordToDatabaseParser(filteredData) {

    const fcount = filteredData.length
    // const kcount = keywords.length
    const url_list = []
    // console.log(count)
    // let format = mime.extension(dist.format)

    for (let i = 0; i < fcount; i++) {
        let rdist = filteredData[i]
        if (rdist != undefined) {
            let temp = {
                url: rdist[0]['accessURL'],
                format: rdist[0]['format'],
                mediatype: rdist[0]['mediaType'],
                identifier: rdist[3],
                dataset_id: rdist[1],
                title: rdist[2]
            }
            console.log()
            // if(i % 10 == 0) console.log(dist[i])
            url_list.push(temp)
            // if(i % 10== 0) console.log(`print in rdf-parser: ${temp}`)
        }
    }
    return url_list
}

function odsKeywordDatasetParser(dataset, keywords) {

    // const dataset = catalog.dataset
    const count = dataset.length
    const kcount = keywords.length
    const results = []
    // console.log(count)
    for (let i = 0; i < count; i++) {
        let exist = dataset[i]
        let tag = dataset[i]['keyword']
        if(i == 0) console.log(tag)
        if (tag != undefined) {
            // console.log(tag)
            const tag_lower = tag.map(element => {
                return element.toLowerCase()
            })
            // if(i % 100 == 0) console.log(tag)
            for (let j = 0; j < kcount; j++) {
                if (tag != undefined) {
                    // let key = keywords[j]
                    // key.toString().toUpperCase()
                    // console.log(key)
                    if (tag_lower.indexOf(keywords[j]) != -1) {
                        let temp = {
                            dataset: {
                                id: "string",
                                url: dataset[i]['landingPage'],
                                title: dataset[i]['title'],
                                issued: dataset[i]['modified'],
                                modified: dataset[i]['modified'],
                                identifier: dataset[i]['identifier'],
                                keyword: keywords[j]
                            },
                            distribution: dataset[i]['distribution']
                        }
                        // console.log(temp)
                        results.push(temp)
                        // if(i == 1) console.log(`print in rdf-parser: ${temp}`)
                    }
                }
            }
        }
    }
    return results
}

function odsDatasetParser(catalog, format) {
    const data = catalog
    const dist = data['rdf:RDF']['rdf:Description']
    // const dataset_count = dataset.length
    // console.log(dist)
    if (dist) {
        const count = Object.keys(dist).length;
        console.log(`Number of Distribution in Catalog: ${count}`)
        let i = 0
        let j = 0
        let url_list = []
        while (i < count) {
            const exist = dist[i]
            if (exist != undefined) {
                if (dist[i]['dct:format'] == format) {
                    if (dist[i]['dcat:accessURL']) {
                        url_list[j] = dist[i]['dcat:accessURL']['@_rdf:resource']
                        j++
                    }
                }
                i++
            } else { i++ }
        }
        return url_list
    } else { return false }
}

function odsKeyFormatDatasetParser(catalog, keywords) {
    const data = catalog
    // console.log(data)
    const dist = data['rdf:RDF']['rdf:Description']
    // const dataset_count = dataset.length
    console.log(dist[0])
    if (dist) {
        const count = Object.keys(dist).length;
        console.log(`Number of Distribution in Catalog: ${count}`)
        let dataset_count = 0
        let distribution_count = 0
        let dataset_list = []
        let dist_list = []
        for (let i =0; i < count; i++) {
            // let obj = JSON.stringify(dist[i])
            let obj = dist[i]
            // if (obj['rdf:type']['@_rdf:resource'])
            if (obj['rdf:type'] != undefined){
                let temp = obj['rdf:type']['@_rdf:resource']
                if (temp.indexOf('dcat#Dataset') != -1) {
                    // console.log(obj)
                    if (obj['dcat:keyword'] != undefined) {
                        let dataset = obj
                        let key = dataset['dcat:keyword']
                        console.log(dataset)
                        console.log(key)
                        for (let k = 0; k < key.length; k++){
                            let tag = key[k]
                            let u_tag
                            if ( k == 0) { 
                                console.log(dataset)
                                console.log(tag)
                            }
                            if (typeof(tag) == 'string') u_tag = tag.toUpperCase()
                            for (let j = 0; j < keywords.length; j++) {
                                let keyword = keywords[j]
                                let u_key = keyword.toUpperCase()
                                if(u_tag.indexOf(u_key) != -1){
                                    let temp = {
                                        dataset: {
                                            id: "string",
                                            url: dataset['landingPage'],
                                            title: dataset['dct:title'],
                                            issued: dataset[i]['modified'],
                                            modified: dataset[i]['modified'],
                                            identifier: dataset['dct:identifier'],
                                            keyword: keywords[j]
                                        },
                                        distribution: dataset['distribution']
                                    }
                                    dataset_count++
                                    dataset_list.push(temp)
                                    // console.log(temp)
                                }
                            }
                        }
                    }
                }
            }
        }
        return dataset_list
    } else { return false }
}