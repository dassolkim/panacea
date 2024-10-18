const { dsaValidator } = require('./DSA_validator')
const config = require('./config/openDataConfig')
const flc = require('./dataHarvester/formatLogicController')
const mklake = require('./dataLakeConstructor/S3-handler')

async function main() {
    const USsourceInfo = {
        defaultUrl: config.USdefaultUrl,
        type: 'catalog',
        name: 'us_catalog',
        publisher: 'USA', // ODLC/US
        site: 'usa',
        realPublisher: 'data.gov'
    }
    const UKsourceInfo = {
        defaultUrl: config.UKdefaultUrl,
        type: 'catalog',
        name: 'uk_catalog',
        publisher: 'UK', // ODLC/US
        site: 'uk',
        realPublisher: 'data.gov.uk'
    }
    const NYsourceInfo = {
        defaultUrl: config.NYdefaultUrl,
        type: 'catalog',
        name: 'ny_catalog',
        publisher: 'NY', // ODLC/US
        site: 'ny_us',
        realPublisher: 'nycopendata.socrata.com'
    }
    const SFsourceInfo = {
        defaultUrl: config.SFdefaultUrl,
        type: 'catalog',
        name: 'sf_catalog',
        publisher: 'SF', // ODLC/US
        site: 'sf_us',
        realPublisher: 'data.sfgov.org'
    }
    const HHSsourceInfo = {
        defaultUrl: config.HHSdefaultUrl,
        type: 'catalog',
        name: 'hhs_catalog',
        publisher: 'HHS', // ODLC/US
        site: 'hhs_us',
        realPublisher: 'healthdata.gov'
    }
    const USEDsourceInfo = {
        defaultUrl: config.USEDdefaultUrl,
        type: 'catalog',
        name: 'used_catalog',
        publisher: 'USED', // ODLC/US
        site: 'used_us',
        realPublisher: 'data.ed.gov'
    }
    const DCsourceInfo = {
        defaultUrl: config.WDCdefaultUrl,
        type: 'catalog',
        name: 'wdc_catalog',
        publisher: 'WDC', // ODLC/US
        site: 'wdc_us',
        realPublisher: 'opendata.dc.gov'
    }
    const MPsourceInfo = {
        defaultUrl: config.MPdefaultUrl,
        type: 'catalog',
        name: 'mp_catalog',
        publisher: 'MP', // ODLC/US
        site: 'mp_us',
        realPublisher: 'opendata.minneapolismn.gov'
    }
    const LCsourceInfo = {
        defaultUrl: config.LSdefaultUrl,
        type: 'catalog',
        name: 'lc_catalog',
        publisher: 'LC', // ODLC/US
        site: 'lc_uk',
        realPublisher: 'data.leicester.gov.uk'
    }
    const ALsourceInfo = {
        defaultUrl: config.ALdefaultUrl,
        type: 'catalog',
        name: 'al_catalog',
        publisher: 'AL', // Junar
        site: 'al_us',
        realPublisher: 'data.arlingtonva.us'
    }
    const PAsourceInfo = {
        defaultUrl: config.PAdefaultUrl,
        type: 'catalog',
        name: 'pa_catalog',
        publisher: 'PA', // Junar
        site: 'pa_us',
        realPublisher: 'data.cityofpaloalto.org'
    }
    const IEsourceInfo = {
        defaultUrl: config.IEdefaultUrl,
        type: 'catalog',
        name: 'ie_catalog',
        publisher: 'IE',
        site: 'ireland',
        realPublisher: 'data.gov.ie'
    }

    // const catalog = await dd.downloadAllCatalog(USsourceInfo, 10)

    const keywords = ['covid-19', 'health'] // health
    // const keywords = ['students', 'teachers'] // 'special-education'
    // const keywords = ['special-education', 'disabilities'] // 'special-education', 'middle-school', 'high-school'
    // const keywords = ['population', 'demography'] // uk: dataset(147), distribution(1079)
    // const keywords = ['disability', 'special-education'] // uk: dataset(147), distribution(1079)
    // const keywords = ['education', 'middle-school', 'high-school']
    // const keywords = ['election', 'voting', 'candidate']
    // const keywords = ['climate-change', 'climate', 'climate-emergency']
    const condition = 'AND'
    const format = 'csv' // GeoJSON, JSON, CSV, XML
    const usBucket = 'us-ckan'
    const ieBucket = 'ie-ckan'
    const nyBucket = 'ny-socrata'
    const hhsBucket = 'hhs-us-dkan'
    const dcBucket = 'wdc-us-arcgis'
    const lcBucket = 'lc-uk-opendatasoft'
    const usedBucket = 'education'
    const sfBucket = 'sf-socrata'
    const ukBucket = 'uk-ckan'

    // AS-IS (before version of metadata manager)
    // await dd.downloadDomainUrls(USsourceInfo, keywords, 1)

    /**
     * Keyword & Format
     * US CKAN
     * 
     */
    // await flc.ckanFormatBasedLogic(USsourceInfo, format, keywords, 100)
    // await flc.ckanKeywordBasedLogic(USsourceInfo, keywords, 100)
    await mklake.createDomainDataLakes(USsourceInfo, keywords, usBucket)


    /**
     * Keyword & Format
     * Ireland CKAN
     */
    // await flc.ckanFormatBasedLogic(IEsourceInfo, format, keywords, 177)
    // await flc.ckanKeywordBasedLogic(IEsourceInfo, keywords, 177)
    // await mklake.createDomainDataLakes(IEsourceInfo, keywords, ieBucket) // check rp and start index


    // add metadata manager logic --> stage (gathering, extraction)
    // await klc.ckanKeywordBasedLogic(USsourceInfo, keywords, 2)

    // add socrata processing logic
    // await dd.socrataKeywordBasedLogic(NYsourceInfo, keywords, 1)

    /**
     * UK CKAN
     * (1) keyword and format 
     * (2) only keyword
     */
    // await flc.ckanFormatBasedLogic(UKsourceInfo, format, keywords, 100)
    // await mklake.createFormatDataLakes(UKsourceInfo, keywords, format, ukBucket)
    // await flc.ckanKeywordBasedLogic(UKsourceInfo, keywords, 581) // 180, 58
    // await mklake.createDomainDataLakes(UKsourceInfo, keywords, ukBucket)

    /**
     * NY (US) Socrata
     */
    // await flc.socrataKeywordBasedLogic(NYsourceInfo, keywords, 1)
    // await mklake.createDomainDataLakes(NYsourceInfo, keywords, nyBucket)

    /**
     * SF (US) Socrata
     */
    // await flc.socrataKeywordBasedLogic(SFsourceInfo, keywords, 1)
    // await mklake.createDomainDataLakes(SFsourceInfo, keywords, sfBucket)

    /**
     * WDC ArcGIS
     */
    // await flc.arcgisKeywordBasedLogic(DCsourceInfo, keywords, 1)
    // await mklake.createDomainDataLakes(DCsourceInfo, keywords, dcBucket)

    /**
     * Minneapolismn (US) ArcGIS
     */
    // await flc.arcgisKeywordBasedLogic(MPsourceInfo, keywords, 1)
    // await mklake.createDomainDataLakes(MPsourceInfo, keywords, dcBucket)

    // add ods processing logic
    // await dd.odsKeywordBasedLogic(LCsourceInfo, keywords, 1)

    // add metadata manager logic --> stage (storing)
    // await mklake.createDomainDataLakes(USsourceInfo, keywords, usBucket)
    // CKAN format-based logic


    /**
     * HHS DKAN
     */
    // await flc.dkanKeywordBasedLogic(HHSsourceInfo, keywords, 1)
    // await mklake.createDomainDataLakes(HHSsourceInfo, keywords, hhsBucket) // 

    /**
     * USED DKAN
     */
    // await flc.dkanKeywordBasedLogic(USEDsourceInfo, keywords, 1)
    // await mklake.createDomainDataLakes(USEDsourceInfo, keywords, usedBucket) // 

     /**
     * Leicester city (UK) Opendatasoft
     */
    // await flc.odsKeywordBasedLogic(LCsourceInfo, keywords, 1)
    // await flc.socrataKeywordBasedLogic(LCsourceInfo, keywords, 1)
    // await mklake.createDomainDataLakes(LCsourceInfo, keywords, lcBucket) // 

    /**
     * AL Junar
     */
    // await flc.dkanKeywordBasedLogic(ALsourceInfo, keywords, 1)
    // await mklake.createDomainDataLakes(USEDsourceInfo, keywords, usedBucket) // 
    /**
     * PA Junar
     */
    // await flc.dkanKeywordBasedLogic(PAsourceInfo, keywords, 1)
    // await mklake.createDomainDataLakes(HHSsourceInfo, keywords, hhsBucket) // 200
    // await mklake.createDomainDataLakes(LCsourceInfo, keywords, lcBucket)

    /**
     * DSA validator
     * US, UK, IE (CKAN)
     * 
     */
    // await flc.ckanFormatBasedLogic(USsourceInfo, format, keywords, 100)
    // await flc.ckanKeywordBasedLogic(USsourceInfo, keywords, 2394)
    // await dsaValidator(USsourceInfo, keywords, usBucket)

}
if (require.main == module) {
    main()
}