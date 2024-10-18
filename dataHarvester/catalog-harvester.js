const config = require('../config/openDataConfig')
const dd = require('./domain-downloader')
const path = require('path')
// const dataDir = path.join('C:/Users/kimds/nodeProject', 'data/')



async function US_test(){
    const USsourceInfo = {
        defaultUrl: config.USdefaultUrl,
        type: 'catalog',
        name: 'us_catalog',
        publisher: 'USA'
    }

    const catalog = await dd.downloadAllCatalog(USsourceInfo, 2549)

    const keywords = ['covid-19', 'health'] // health
    const condition = 'AND'
    const format = 'CSV'
    // await dd.downloadDomainUrls(USsourceInfo, keywords, 1)
    // await dd.downloadFormatUrls(USsourceInfo, format, 1)
    // await dd.downloadDomainUrls(USsourceInfo, keywords, condition, 1)



}
US_test()

async function ckan_test(){
    const USsourceInfo = {
        defaultUrl: config.USdefaultUrl,
        type: 'catalog',
        name: 'us_catalog',
        publisher: 'ckan_test'
    }
    /**
     * Get RDF Catalog Test
     */
    await dd.downloadAllCatalog(USsourceInfo, 20)
    // console.log(`######### Collect ${USsourceInfo.name} on Web (data portal) #########`)
    // console.log(catalog)
        // downloader test
    // add JSON extraction
    await dd.downloadAllUrls(USsourceInfo, 'CSV', 20)
    // const url = await dd.downloadAllUrls(USsourceInfo, 'CSV', 1000)

    // return url
}
// ckan_test()

async function HPC01_ckan(){
    const USsourceInfo = {
        defaultUrl: config.USdefaultUrl,
        type: 'catalog',
        name: 'hpc_catalog',
        publisher: 'HPC01_CKAN'
    }
    /**
     * Get RDF Catalog Test
     */
    // await dd.downloadAllCatalog(USsourceInfo, 5)
    // console.log(`######### Collect ${USsourceInfo.name} on Web (data portal) #########`)
    // console.log(catalog)
        // downloader test
    // add JSON extraction
    await dd.downloadAllUrls(USsourceInfo, 'CSV', 5)
    // const url = await dd.downloadAllUrls(USsourceInfo, 'CSV', 1000)

    // return url
}
// HPC01_ckan()

async function comparitive_test(){
    const USsourceInfo = {
        defaultUrl: config.USdefaultUrl,
        type: 'catalog',
        name: 'us_catalog',
        publisher: 'US_with_CE'
    }
    /**
     * Get RDF Catalog Test
     */
    // const catalog = await dd.downloadAllCatalog(USsourceInfo, 1000)
    console.log(`######### Collect ${USsourceInfo.name} on Web (data portal) #########`)
    // console.log(catalog)
        // downloader test
    // add JSON extraction
    // const url = await dd.downloadAllUrls(USsourceInfo, 'CSV', 3436)
    const url = await dd.downloadAllUrls(USsourceInfo, 'CSV', 1000)

    return url
}

// comparitive_test()




async function canadatest(){
    const CAsourceInfo = {
        defaultUrl: config.CAdefaultUrl,
        type: 'catalog',
        name: 'ca_catalog',
        publisher: 'CA'
    }
    /**
     * Get RDF Catalog Test
     */
    const catalog = await dd.downloadAllCatalog(CAsourceInfo, 30)
    // console.log(`######### Collect ${CAsourceInfo.name} on Web (data portal) #########`)
    // console.log(catalog)
    
    // downloader test
    // const url = await dd.downloadAllUrls(CAsourceInfo, 'GEOJSON', 320)
    console.log(`######### Collect ${CAsourceInfo.name} on Web (data portal) #########`)
    // return url
}

// canadatest()

async function uktest(){
    const UKsourceInfo = {
        defaultUrl: config.UKdefaultUrl,
        type: 'catalog',
        name: 'uk_catalog',
        publisher: 'UK'
    }
    /**
     * Get RDF Catalog Test
     */
    // console.log(`######### Collect ${UKsourceInfo.name} on Web (data portal) #########`)
    const catalog = await dd.downloadAllCatalog(UKsourceInfo, 581)
    // console.log(catalog)
    
    // downloader test
    // console.log(`######### Collect ${UKsourceInfo.name} on Web (data portal) #########`)
    // const url = await dd.downloadAllUrls(UKsourceInfo, 'GEOJSON', 521)
    // return url
}
// uktest()

async function dkan_test(){
    const OKsourceInfo = {
        defaultUrl: config.OKdefaultUrl,
        type: 'catalog',
        name: 'ok_catalog',
        publisher: 'OK_dkan'
    }
    /**
     * Get RDF Catalog Test
     */
    console.log(`######### Collect ${OKsourceInfo.name} on Web (data portal) #########`)
    const catalog = await dd.downloadAllCatalog(OKsourceInfo, 20)
    // console.log(catalog)
    
    // downloader test
    // console.log(`######### Collect ${OKsourceInfo.name} on Web (data portal) #########`)
    // const url = await dd.downloadAllUrls(OKsourceInfo, 'CSV', 5)
    // return url
}

// dkan_test()

async function NY_socrata_test(){
    const NYsourceInfo = {
        defaultUrl: config.NYdefaultUrl,
        type: 'catalog',
        name: 'ny_catalog',
        publisher: 'Socrata',
        portal: 'NY',
        page: 1
    }
    /**
     * Get RDF Catalog Test
     */
    // console.log(`######### Collect ${NYsourceInfo.name} on Web (data portal) #########`)
    const catalog = await dd.downloadAllCatalog(NYsourceInfo, 1)
    // console.log(catalog)
    
    // downloader test
    // console.log(`######### Collect ${NYsourceInfo.name} on Web (data portal) #########`)
    // const url = await dd.downloadSocrataDataset(NYsourceInfo, 'JSON', 1)
    // const url = await dd.downloadSocrataDataset(NYsourceInfo, 'CSV', 1)

    // return url
}
// NY_socrata_test()

async function SF_socrata_test(){
    const SFsourceInfo = {
        defaultUrl: config.SFdefaultUrl,
        type: 'catalog',
        name: 'sf_catalog',
        publisher: 'Socrata',
        page: 1,
        portal: 'SF'
    }
    /**
     * Get RDF Catalog Test
     */
    console.log(`######### Collect ${SFsourceInfo.name} on Web (data portal) #########`)
    const catalog = await dd.downloadAllCatalog(SFsourceInfo, 1)
    // console.log(catalog)
    
    // downloader test
    // console.log(`######### Collect ${SFsourceInfo.name} on Web (data portal) #########`)
    // const url = await dd.downloadSocrataDataset(SFsourceInfo, 'JSON', 1)
    // const url = await dd.downloadSocrataDataset(NYsourceInfo, 'CSV', 1)

    // return url
}
// SF_socrata_test()

async function LC_ods_test(){
    const ODSsourceInfo = {
        defaultUrl: config.LSdefaultUrl,
        type: 'catalog',
        name: 'lc_catalog',
        publisher: 'Opendatasoft',
        city: 'LC',
        page: 1
    }
    /**
     * Get RDF Catalog Test
     */
    const catalog = await dd.downloadAllCatalog(ODSsourceInfo, 1)
    // console.log(`######### Collect ${ODSsourceInfo.name} on Web (data portal) #########`)
 
    // const url = await dd.downloadODSDataset(ODSsourceInfo, 'JSON', 1)

    // return url
}

// LC_ods_test()

async function BS_ods_test(){
    const ODSsourceInfo = {
        defaultUrl: config.BSdefaultUrl,
        type: 'catalog',
        name: 'bs_catalog',
        publisher: 'Opendatasoft',
        city: 'BS',
        page: 1
    }
    /**
     * Get RDF Catalog Test
     */
    const catalog = await dd.downloadAllCatalog(ODSsourceInfo, 1)
    // console.log(`######### Collect ${ODSsourceInfo.name} on Web (data portal) #########`)
 
    // const url = await dd.downloadODSDataset(ODSsourceInfo, 'JSON', 1)

    // return url
}

// BS_ods_test()

async function JC_ods_test(){
    const JCsourceInfo = {
        defaultUrl: config.JSdefaultUrl,
        type: 'catalog',
        name: 'jc_catalog',
        publisher: 'Opendatasoft',
        city: 'JC',
        page: 1
    }
    /**
     * Get RDF Catalog Test
     */
    // await dd.downloadAllCatalog(JCsourceInfo, 1)
    // console.log(`######### Collect ${ODSsourceInfo.name} on Web (data portal) #########`)
 
   await dd.downloadODSDataset(JCsourceInfo, 'JSON', 1)

    // return url
}

// JC_ods_test()

async function IE_ckan_test(){
    const IEsourceInfo = {
        defaultUrl: config.IEdefaultUrl,
        type: 'catalog',
        name: 'ie_catalog',
        publisher: 'IE'
    }
    /**
     * Get RDF Catalog Test
     */
    const catalog = await dd.downloadAllCatalog(IEsourceInfo, 177)
    // console.log(`######### Collect ${IEsourceInfo.name} on Web (data portal) #########`)

    // const url = await dd.downloadAllUrls(IEsourceInfo, 'JSON', 142)
    // const url = await dd.downloadAllUrls(USsourceInfo, 'CSV', 1000)

}

// IE_ckan_test()

async function HHS_dkan_test(){
    const HHSsourceInfo = {
        defaultUrl: config.HHSdefaultUrl,
        type: 'catalog',
        name: 'hhs_catalog',
        portal: 'HHS',
        publisher: 'DKAN_json',
        page: 1
    }
    /**
     * Get RDF Catalog Test
     */
    // console.log(`######### Collect ${HHSsourceInfo.name} on Web (data portal) #########`)
    const catalog = await dd.downloadAllCatalog(HHSsourceInfo, 1)
    // console.log(catalog)
    
    // downloader test
    // console.log(`######### Collect ${HHSsourceInfo.name} on Web (data portal) #########`)
    // const url = await dd.downloadSocrataDataset(HHSsourceInfo, 'JSON', 1)
    // const url = await dd.downloadSocrataDataset(NYsourceInfo, 'CSV', 1)

    // return url
}
// HHS_dkan_test()

async function WDC_test(){
    const page = 1
    const WDCsourceInfo = {
        defaultUrl: config.WDCdefaultUrl,
        type: 'catalog',
        name: 'wdc_catalog',
        publisher: 'ArcGIS',
        portal: 'WDC',
        page: page
    }
    /**
     * Get RDF Catalog Test
     */
    const catalog = await dd.downloadAllCatalog(WDCsourceInfo, 1)
    // console.log(`######### Collect ${USsourceInfo.name} on Web (data portal) #########`)
    // add JSON extraction
    // await dd.downloadArcGISDataset(WDCsourceInfo, 'JSON', page)

    // return url
}
// WDC_test()

async function MP_test(){
    const page = 1
    const MPsourceInfo = {
        defaultUrl: config.MPdefaultUrl,
        type: 'catalog',
        name: 'mp_catalog',
        publisher: 'ArcGIS',
        portal: 'MP',
        page: page
    }
    /**
     * Get RDF Catalog Test
     */
    const catalog = await dd.downloadAllCatalog(MPsourceInfo, 1)
    // console.log(`######### Collect ${USsourceInfo.name} on Web (data portal) #########`)
    // add JSON extraction
    // await dd.downloadArcGISDataset(MPsourceInfo, 'JSON', page)

    // return url
}
// MP_test()

async function USDA_dkan_test(){
    const page = 1
    const USDAsourceInfo = {
        defaultUrl: config.USDAdefaultUrl,
        type: 'catalog',
        name: 'usda_catalog',
        portal: 'USDA',
        publisher: 'DKAN_json',
        page: page
    }
    /**
     * Get RDF Catalog Test
     */
    // await dd.downloadAllCatalog(USDAsourceInfo, 1)
 
    await dd.downloadSocrataDataset(USDAsourceInfo, 'JSON', page)
}
// USDA_dkan_test()

async function PA_junar_test(){
    const page = 1
    const PAsourceInfo = {
        defaultUrl: config.PAdefaultUrl,
        type: 'catalog',
        name: 'pa_catalog',
        publisher: 'Junar',
        portal: 'PA',
        page: page
    }
    /**
     * Get RDF Catalog Test
     */
    await dd.downloadAllCatalog(PAsourceInfo, 1)
    // console.log(`######### Collect ${USsourceInfo.name} on Web (data portal) #########`)
    // add JSON extraction
    // await dd.downloadSocrataDataset(PAsourceInfo, 'CSV', page)

    // return url
}
// PA_junar_test()

async function AL_junar_test(){
    const page = 1
    const ALsourceInfo = {
        defaultUrl: config.ALdefaultUrl,
        type: 'catalog',
        name: 'al_catalog',
        publisher: 'Junar',
        portal: 'AL',
        page: page
    }
    /**
     * Get RDF Catalog Test
     */
    await dd.downloadAllCatalog(ALsourceInfo, 1)
    // console.log(`######### Collect ${USsourceInfo.name} on Web (data portal) #########`)
    // add JSON extraction
    // await dd.downloadSocrataDataset(ALsourceInfo, 'JSON', page)

    // return url
}
// AL_junar_test()

async function used_dkan_test(){
    const USEDsourceInfo = {
        defaultUrl: config.USEDdefaultUrl,
        type: 'catalog',
        name: 'used_catalog',
        publisher: 'DKAN_json',
        portal: 'USED',
        page: 1
    }
    /**
     * Get RDF Catalog Test
     */
    await dd.downloadAllCatalog(USEDsourceInfo, 1)
    // console.log(catalog)
    
    // downloader test
    // console.log(`######### Collect ${NYsourceInfo.name} on Web (data portal) #########`)
    // const url = await dd.downloadSocrataDataset(NYsourceInfo, 'JSON', 1)
    // const url = await dd.downloadSocrataDataset(NYsourceInfo, 'CSV', 1)

    // return url
}
// used_dkan_test()