require('dotenv').config()
var _ = require('lodash')
const axios = require('axios').default
const { Sequelize, Op, Model, DataTypes } = require('sequelize')
const { snakeCase } = require('change-case')
var Airtable = require('airtable');
const EventEmitter = require('events')
const { createClient } = require('@supabase/supabase-js')
const { promisify } = require('util')
const sleep = promisify(setTimeout)


class Emitter extends EventEmitter {}
const EVENTS = new Emitter()
WIP = {}

var airtableMeta
var airtable
var baseName
var META
var supabase
var fieldTypesIgnore
var BASES
var BASE = {}
var MODELS = {}
var RECS = {/** rec123sdfkj: {foo: 'bar'} */}
var RECS_tables_done
const FIELD_TYPES = { // TODO create test table with fiesl named like all types
  autoNumber:			        {type: DataTypes.INTEGER},
  barcode:			          {type: DataTypes.JSONB},
  button:			            {type: DataTypes.JSONB},
  checkbox:			          {type: DataTypes.BOOLEAN},
  count:			            {type: DataTypes.INTEGER},
  createdBy:			        {type: DataTypes.JSONB},
  createdTime:			      {type: DataTypes.DATE},
  currency:			          {type: DataTypes.FLOAT},
  date:			              {type: DataTypes.DATEONLY},
  dateTime:			          {type: DataTypes.DATE},
  duration:			          {type: DataTypes.INTEGER},
  email:			            {type: DataTypes.STRING},
  formula:			          {type: DataTypes.TEXT},
  lastModifiedBy:			    {type: DataTypes.JSONB},
  lastModifiedTime:	      {type: DataTypes.DATE},
  multilineText:			    {type: DataTypes.TEXT},
  multipleAttachments:    {type: DataTypes.JSONB},
  multipleCollaborators:	{type: DataTypes.JSONB},
  multipleLookupValues:	  {type: DataTypes.JSONB},
  multipleRecordLinks:    {type: DataTypes.ARRAY(DataTypes.STRING)},
  multipleSelects:	      {type: DataTypes.ARRAY(DataTypes.STRING)},
  number:			            {type: DataTypes.INTEGER},
  percent:			          {type: DataTypes.FLOAT},
  phoneNumber:			      {type: DataTypes.STRING},
  rating:			            {type: DataTypes.INTEGER},
  richText:			          {type: DataTypes.TEXT},
  rollup:			            {type: DataTypes.TEXT},
  singleCollaborator:		  {type: DataTypes.JSONB},
  singleLineText:			    {type: DataTypes.TEXT},
  singleSelect:			      {type: DataTypes.STRING},
  url:			              {type: DataTypes.STRING},
}
// TODO move inside class, use arrow function to access via this?
var lastRefresh
var base


module.exports = class Supair {
  constructor({
    supabaseConnectionString,
    airtableApiKey,
    airtableMetadataApiKey,
    airtableBaseName,
    airtableIgnoreFieldTypes,
  }) {    
    this.sequelize = new Sequelize(supabaseConnectionString, {
      logging: false,
      define: {
        freezeTableName: true
      }
    })
    this.sequelize.authenticate().then(() => {
        console.info('INFO - sequelize: database connected.')
    })
    .catch(err => {
      console.error('ERROR - sequelize: unable to connect to the database:', err)
    })
    baseName = airtableBaseName
    fieldTypesIgnore = airtableIgnoreFieldTypes || ['formula', 'multipleLookupValues', 'rollup']
    airtable = new Airtable({apiKey: airtableApiKey})
    airtableMeta = axios.create({
      baseURL: 'https://api.airtable.com/v0/meta/',
      timeout: 20000,
      headers: {
        "X-Airtable-Client-Secret": `${airtableMetadataApiKey}`,
        "Authorization": `Bearer ${airtableApiKey}`
      }
    })
    EVENTS.on('workStarted', (work, note, silent) => {
      WIP[work] = true
      if (!silent)
        console.log(`⌛ ${work}: ${note || ''}`)
    })
    EVENTS.on('workFinished', (work, note, silent) => {
      delete WIP[work]
      if (!silent)
        console.log(`✅️ ${work}: ${note || ''}`)
    })
    console.log(`Create supabase client using SUPABASE_URL ${process.env.SUPABASE_URL}`)
    supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY)  
  }

  async getMetaData() { //returns hybrid of enriched metadata and current Airtable metadata for further enrichment by the user
    const basesMeta = await airtableMeta.get('bases')
    META = _.find(basesMeta.data.bases, {name: baseName})
    if (!META || !META.id) throw `Failed finding metadata for base '${baseName}'`
    const tablesMeta = await airtableMeta.get(`bases/${META.id}/tables`)
    if (!tablesMeta || !tablesMeta.data.tables) throw `Failed to get tables metadata for base '${baseName}'`
    META.tables = _.keyBy(tablesMeta.data.tables, 'name')
    for (let tableName in META.tables) {
      META.tables[tableName].fields = _.keyBy(META.tables[tableName].fields, 'name')
      delete META.tables[tableName].views
    }
    return META
  }

  async createSqlSchema(metaData) {
    META = metaData || META
    console.log(META)
    for (const TblNm in META.tables) {
      const Table = META.tables[TblNm]
      var modelFields = {
        recid: {
          type: DataTypes.STRING,
          primaryKey: true,
        },
      }
      for (const FldNm in Table.fields) {
        const Field = Table.fields[FldNm]
        if (fieldTypesIgnore.includes(Field.type)) {
          continue
        }
        modelFields[FldNm] = {
          type: FIELD_TYPES[Field.type].type,
        }
      }
      console.log(`Define sqlz model: ${TblNm}`)
      MODELS[TblNm] = this.sequelize.define(TblNm, modelFields)
    }
    console.log("Sync all sqlz models to SQL....")
    await this.sequelize.sync({alter: true}) // ❗
    console.log("...all sqlz models were synced to SQL.")
    console.log(Object.keys(this.sequelize.models))
  }

  async syncData({at2pg, pg2at}) {
    RECS_tables_done = new Set()
    console.log(`Loading all data from base ${META.id} to detect links`)
    for (let tableName in META.tables) {
      airtable.base(META.id)(tableName).select({
        //maxRecords: 100
      }).eachPage(async function page(records, fetchNextPage) {
        for (let record of records) {
          RECS[record.id] = {
            _recordId: record.id,
            _tableName: tableName,
            ...record.fields
          }
        }
        fetchNextPage()
      }, async function done(err) {
        if (err) return console.error(err)
        await sleep(3000)
        console.log(`RECS_tables_done: ${tableName}`)
        RECS_tables_done.add(tableName)
      })
    }
  }

  async createReferences() {
    if (!META.tables || RECS_tables_done.size != _.size(META.tables))
      return `...meta data is still being initialized. So far RECS selected ${_.size(RECS)}\n`  
    for (let tableName in META.tables) {
      //if (tableName != 'Venues') continue // TMP
      const table = META.tables[tableName]
      const allRecs = _.filter(RECS, {_tableName: tableName})
      console.log(`${tableName} has ${_.size(allRecs)} records. FK fields:`)
      for (let fieldName in table.fields) {
        const field = table.fields[fieldName]
        if (field.type == 'multipleRecordLinks') {
          field._relation = {}
          const anyRecWithLink = _.find(allRecs, rec => (rec[fieldName] && rec[fieldName].length > 0))
          field._relation.table = RECS[anyRecWithLink[fieldName][0]]._tableName
          const linksToMany = _.find(allRecs, rec => (rec[fieldName] && rec[fieldName].length > 1))
          if (linksToMany) {
            field._relation.cardinality = 'many'
          } else {
            field._relation.cardinality = 'one'
          }
          console.log(field)
        }
      }
    }
    let { data: sysconf, error } = await supabase.from('sysconf').select('*')
    if (error) throw error
    if (sysconf && sysconf.length) {
      META = _.merge(META, sysconf[0].metadata)
    }
  }

  async init_OLD(force = true, skipData = true) {

    
    if (skipData) return

    base = airtable.base(BASE.id)
    console.log(`Loading all data from Base id ${BASE.id}`)
    for (const [TblNm, Table] of Object.entries(BASE.TABLES)) {
      EVENTS.emit('workStarted', TblNm, 'initial load')
      base(TblNm).select({}).eachPage(async function page(records, fetchNextPage) {
        var sqlRecs = []
        for (let record of records) {
          var sqlRec = {
            recid: record.id,
            ...record.fields
          }
          sqlRecs.push(sqlRec)
        }
        try {
          const inserted = await MODELS[TblNm].bulkCreate(sqlRecs, { validate: true })
        } catch (error) {
          console.log(`ERROR while bulkInserting into ${TblNm}`)
          console.log(error)
          process.exit()
        }
        fetchNextPage()
      }, async function done(err) {
        if (err) { console.error(err); return; }
        EVENTS.emit('workFinished', TblNm, 'initial load')
      })
      lastRefresh = new Date()
    }
  }
  async keepInSync(intervalInSec = 30) {
    setInterval(function(){
      const worksInProgress = Object.keys(WIP).length
      const timeStamp = new Date().toTimeString().slice(0,8)
      if (worksInProgress > 0) {
        console.log(`❌️ ${timeStamp}: attempting delta load from Airtable -----> ${worksInProgress} WORKs in progress => try next interval!`)
        return
      }
      if (!lastRefresh) {
        console.log(`last refresh time not set yet due to incomplete init, skip, try next interval!`)
        return
      }
      // ! need to generate formula here, and not on each select, because lastRefresh is updated before all selects eventually finish
      const formula = `IS_AFTER(LAST_MODIFIED_TIME(), '${lastRefresh.toISOString()}')`
      console.log(`⏩️ ${timeStamp}: No WORKs in progress => scan for records in Airtable using formula: ${formula}`)
      for (const [TblNm, Table] of Object.entries(BASE.TABLES)) {
        EVENTS.emit('workStarted', `${TblNm}_delta`, '', true)
        base(TblNm).select({
          filterByFormula: formula
        }).eachPage(async function page(records, fetchNextPage) {
          var sqlRecs = []
          //console.log(`...${records.length} ${TblNm} modified records selected from Airtable`)
          for (let record of records) {
            var sqlRec = {
              recid: record.id,
              ...record.fields
            }
            sqlRecs.push(sqlRec)
          }
          if (sqlRecs && sqlRecs.length) {
            const flds = Object.keys(records[0].fields)
            try {
              const upserterd = await MODELS[TblNm].bulkCreate(
                sqlRecs,
                {updateOnDuplicate: flds}
              ) // UPSERT!
              console.log(`${upserterd.length} ${TblNm} modified recs upserted in SQL`)              
            } catch (error) {
              console.log(`ERROR while bulk upsert`)
              console.log(error)
              process.exit()
            }
          }          
          fetchNextPage()          
        }, async function done(err) {
          if (err) { console.error(err); return; }
          EVENTS.emit('workFinished', `${TblNm}_delta`, '', true)
        })
      }
      lastRefresh = new Date() // TODO save per table in done()
    }, intervalInSec * 1000)    
  }
}

