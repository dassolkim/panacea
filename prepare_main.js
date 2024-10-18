const config = require('../config/openDataConfig')
const dd = require('./dataHarvester/domain-downloader')
const mklake = require('./dataLakeConstructor/S3-handler')

async function main() {
    const USsourceInfo = {
        defaultUrl: config.USdefaultUrl,
        type: 'catalog',
        name: 'us_catalog',
        publisher: 'ODLC/US', // ODLC/US
        site: 'us',
        realPublisher: 'data.gov'
    }
    const NYsourceInfo = {
        defaultUrl: config.NYdefaultUrl,
        type: 'catalog',
        name: 'ny_catalog',
        publisher: 'ODLC/NY', // ODLC/US
        site: 'ny_us',
        realPublisher: 'nycopendata.socrata.com'
    }
    const HHSsourceInfo = {
        defaultUrl: config.HHSdefaultUrl,
        type: 'catalog',
        name: 'hhs_catalog',
        publisher: 'ODLC/HHS', // ODLC/US
        site: 'hhs_us',
        realPublisher: 'healthdata.gov'
    }
    const DCsourceInfo = {
        defaultUrl: config.WDCdefaultUrl,
        type: 'catalog',
        name: 'dc_catalog',
        publisher: 'ODLC/DC', // ODLC/US
        site: 'dc_us',
        realPublisher: 'opendata.dc.gov'
    }
    const LCsourceInfo = {
        defaultUrl: config.WDCdefaultUrl,
        type: 'catalog',
        name: 'lc_catalog',
        publisher: 'ODLC/LC', // ODLC/US
        site: 'lc_uk',
        realPublisher: 'data.leicester.gov.uk'
    }

    // const catalog = await dd.downloadAllCatalog(USsourceInfo, 10)

    const keywords = ['covid-19', 'health'] // health
    const condition = 'AND'
    const format = 'GeoJSON' // GeoJSON, JSON, CSV, XML
    const usBucket = 'us-ckan'
    const nyBucket = 'ny-socrata'
    const hhsBucket = 'hhs-us-dkan'
    const dcBucket = 'dc-us-arcgis'
    const lcBucket = 'lc-uk-opendatasoft'

    // AS-IS (before version of metadata manager)
    // await dd.downloadDomainUrls(USsourceInfo, keywords, 1)

    // format based processing logic --> need to apply metadata manager logic
    // await dd.downloadFormatUrls(USsourceInfo, format, 1)

    // add metadata manager logic --> stage (gathering, extraction)
    // await dd.ckanKeywordBasedLogic(USsourceInfo, keywords, 1)

    // add socrata processing logic
    // await dd.socrataKeywordBasedLogic(NYsourceInfo, keywords, 1)

    // add dkan processing logic
    // await dd.dkanKeywordBasedLogic(HHSsourceInfo, keywords, 1)

    // add arcgis processing logic
    // await dd.arcgisKeywordBasedLogic(DCsourceInfo, keywords, 1)

    // add ods processing logic
    // await dd.odsKeywordBasedLogic(LCsourceInfo, keywords, 1)

    // add metadata manager logic --> stage (storing)
    // await mklake.createDomainDataLakes(USsourceInfo, keywords, usBucket)
    // await mklake.createDomainDataLakes(NYsourceInfo, keywords, nyBucket)
    // await mklake.createDomainDataLakes(HHSsourceInfo, keywords, hhsBucket) // 200
    // await mklake.createDomainDataLakes(DCsourceInfo, keywords, dcBucket)
    // await mklake.createDomainDataLakes(LCsourceInfo, keywords, lcBucket)

}
if (require.main == module) {
    main()
}