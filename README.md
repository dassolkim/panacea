# Panacea: An Automatic Framework for Constructing Internet-scale Open Data Lakes

Goal: Constructing Internet-scale open data lakes (ODLs) of specific domain 

Paper link: TBA

## Quick start

### Environment
 - Node: above v.16.14.2
 - Minio: Release.2022-10-08T20-11-00Z 
 - PostgreSQL: v.14.2
 - ORM
   - sequalize: v.6.25.3
   - sequalize-cli: v.6.5.2 

### Running
1. Download this source (git clone)
2. Execute npm install
3. Edit connectConfig.js (Object Storage Info, URL, Bucket, Keywords)
4. node ./metadataManager/init-DB.js (create metadata database for Pancea)
5. node ./prepare-main.js (set data source and harvest all catalog)
6. node ./controller-main.js (pre-processing catalogs and migrate real data to datalakes)