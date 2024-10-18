const fh = require('../fileHandler/file-handler')
const path = require('path')
const defaultPath = path.join('C:/Users/kimds/nodeProject', 'data/')

async function main() {

    const dataDir = defaultPath
    const format = 'CSV'
    const CAurlInfo = {
        name: 'ca_catalog',
        type: 'url',
        format: format.toLowerCase(),
        publisher: 'CA'
    }

    const USurlInfo = {
        name: 'us_catalog',
        type: 'url',
        format: format.toLowerCase(),
        publisher: 'US'
    }

    const OKurlInfo = {
        name: 'ok_catalog',
        type: 'url',
        format: format.toLowerCase(),
        publisher: 'OK_dkan'
    }

    const NYurlInfo = {
        name: 'ny_catalog',
        type: 'url',
        format: format.toLowerCase(),
        publisher: 'Socrata/NY'
    }

    const UKurlInfo = {
        name: 'uk_catalog',
        type: 'url',
        format: format.toLowerCase(),
        publisher: 'UK'
    }

    const IEurlInfo = {
        name: 'ie_catalog',
        type: 'url',
        format: format.toLowerCase(),
        publisher: 'IE'
    }
    const LUSurlInfo = {
        name: 'us_catalog',
        type: 'url',
        format: format.toLowerCase(),
        publisher: 'Latest/US'
    }
    const LCAurlInfo = {
        name: 'ca_catalog',
        type: 'url',
        format: format.toLowerCase(),
        publisher: 'Latest/CA'
    }
    const LUKurlInfo = {
        name: 'uk_catalog',
        type: 'url',
        format: format.toLowerCase(),
        publisher: 'Latest/UK'
    }
    const HHSurlInfo = {
        type: 'url',
        name: 'hhs_catalog',
        portal: 'HHS',
        format: format.toLowerCase(),
        publisher: 'DKAN_json'
    }
    const BSurlInfo = {
        type: 'url',
        name: 'bs_catalog',
        format: format.toLowerCase(),
        publisher: 'Opendatasoft/BS',
    }
    const LCurlInfo = {
        type: 'url',
        name: 'ods_catalog',
        format: format.toLowerCase(),
        publisher: 'Opendatasoft/LC',
    }
    const SFurlInfo = {
        name: 'sf_catalog',
        type: 'url',
        format: format.toLowerCase(),
        publisher: 'Socrata/SF'
    }
    const USDAurlInfo = {
        name: 'usda_catalog',
        type: 'url',
        format: format.toLowerCase(),
        publisher: 'DKAN_json/USDA'
    }
    const MPurlInfo = {
        type: 'url',
        name: 'mp_catalog',
        format: format.toLowerCase(),
        publisher: 'ArcGIS/MP'
    }
    const WDCurlInfo = {
        type: 'url',
        name: 'wdc_catalog',
        format: format.toLowerCase(),
        publisher: 'ArcGIS/WDC'
    }
    const HPCurlInfo = {
        type: 'url',
        name: 'hpc_catalog',
        format: format.toLowerCase(),
        publisher: 'HPC01_CKAN'
    }

    const CAend = 319
    const USend = 3435
    const OKend = 5
    const NYend = 1
    const UKend = 520
    const IEend = 142
    const Latest = 1
    let total_count = 0
    const urlInfo = HPCurlInfo
    const end = OKend
    let remove = 0
    for (let page = 1; page <= end; page++) {
        urlInfo.page = page
        const urls = await fh.readUrls(dataDir, urlInfo)

        if (!urls) {
            console.log(`read data is failed`)
        } else {
            const urlObj = JSON.parse(urls)
            const urlList = urlObj.url
            const count = urlList.length
            // console.log(urlList.length)
            // console.log(typeof (urlList))
            console.log(count)
            for (let i = 0; i < count; i++) {
                const url = urlList[i]
                if (url.endsWith('.pdf') || url.endsWith('.zip'))  { // url.endsWith('.xls')
                    remove++
                    console.log(url)
                    // console.log(i)
                    // urlList.pop(url)
                    urlList[i] = null
                }
            }
            let new_cnt = 0
            
            if (remove != 0) {
                let newList = []
                let a = 0
                for (let j = 0; j < urlList.length; j++) {
                    if (urlList[j] != null) {
                        newList[a] = urlList[j]
                        a++
                    }
                }
                new_cnt = newList.length
                urlInfo.page = page
                urlInfo.count = new_cnt

                const wUrls = await fh.writeUrls(newList, urlInfo)
                // console.log(wUrls)
                if (wUrls) {
                    console.log(`write urls to files is succeeded`)
                }
            } else {
                new_cnt = count
            }
            total_count += new_cnt
            console.log(`Count of newList in ${urlInfo.publisher} page ${page}: ${new_cnt}`)
        }
    }
    console.log(`total ${format.toLowerCase()} files in ${urlInfo.publisher} open data portal page 1-${end}: ${total_count}`)
}

if (require.main == module) {
    main()
}