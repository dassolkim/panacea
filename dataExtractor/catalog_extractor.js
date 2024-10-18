const configInfo = require('../../config/connectConfig')

const fh = require('../fileHandler/file-handler')
const path = require('path')
const defaultPath = path.join('C:/Users/kimds/nodeProject', 'data/')

async function main() {

    /**
     * odl-source/validation logic
     */
    // read urls in file
    const dataDir = defaultPath
    const format = 'csv'
    const publisher = 'CA'
    const page = 1
    const urlInfo = {
        name: 'ca_catalog',
        type: 'url',
        format: format,
        publisher: publisher,
        page: page
    }
    const lastPage = 319

    let global_cnt = 0
    let original_cnt = 0
    let p = page
    let list = []
    while (p <= lastPage) {

        urlInfo.page = p
        const rUrls = fh.readUrls(dataDir, urlInfo)
        if (rUrls == false) {
            console.log(`Zero ${format} files in catalog page ${p}`)
        } else {
            const urlObj = JSON.parse(rUrls)
            const count = urlObj.info.count
            const pg_count = {
                catalog: p,
                count: count
            }
            list.push(pg_count)
            console.log(pg_count)
        }
        p++
 
    }
    console.log(`Number of ${format} files in US portal: ${original_cnt}`)
    console.log(`Number of created sources in US portal ${format} files: ${global_cnt}`)

}
if (require.main == module) {
    main()
}