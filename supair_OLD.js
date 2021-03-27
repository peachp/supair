require('dotenv').config()
const axios = require('axios').default
const { Sequelize, Op, Model, DataTypes } = require('sequelize')
const { snakeCase } = require('change-case')
var Airtable = require('airtable');
const EventEmitter = require('events')


class Emitter extends EventEmitter {}
const EVENTS = new Emitter()
WIP = {}

//const { createClient } = require('@supabase/supabase-js')

function rename(str) {
  //str = str.replace(/ /g, '')
  //str = snakeCase(str)
  return str
}
function arr2obj(arr, keyProp = 'name') {
  var obj = {}
  for (let elem of arr) {
    const key = elem[keyProp]
    if (typeof key === 'number' || typeof key === 'string') {
      obj[key] = {...elem}
      delete obj[key][keyProp]
    } else {
      console.error(`Key ${keyProp} must be an integer or string, got:`, key)
    }
  }
  return obj
}

var BASES
var BASE = {}
var MODELS = {}
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
const ACTIVE_BASES = ['PPW', 'PPW_TEST', 'CMS', ]
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
    
    this.atMetadata = axios.create({
      baseURL: 'https://api.airtable.com/v0/meta/',
      timeout: 20000,
      headers: {
        "X-Airtable-Client-Secret": `${airtableMetadataApiKey}`,
        "Authorization": `Bearer ${airtableApiKey}`
      }
    })
        
    this.airtableBaseName = airtableBaseName
    this.airtableIgnoreFieldTypes = airtableIgnoreFieldTypes || ['formula', 'multipleLookupValues', 'rollup']
    this.airtable = new Airtable({apiKey: airtableApiKey})
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
    // to listen for RT CUD events !!! infinite loop dager: avoid when coming from delta load
    /// do this in startSync ?
    //this.supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY)        
  }

  async init(force = true, skipData = true) {

    try {
      await this.sequelize.authenticate();
      console.log('Connection to Supabase SQL DB has been established successfully.');
    } catch (error) {
      console.error('Unable to connect to the Supabase SQL DB:', error);
      return
    }    

    const basesRes = await this.atMetadata.get('bases')    
    BASES = arr2obj(basesRes.data.bases)
    console.log('BASES', BASES)
    for (let bn in BASES) {
      if (!BASES[bn] || !BASES[bn].id) {
        console.error(`No Base found named ${this.airtableBaseName}`)
      }
      if (!ACTIVE_BASES.includes(bn)) {
        console.log(`Base '${bn}' is not in the list of active bases => skip`)
        continue
      }
      const tablesRes = await this.atMetadata.get(`bases/${BASES[bn].id}/tables`)
      BASES[bn].TABLES = arr2obj(tablesRes.data.tables)
      for (let tableName in BASES[bn].TABLES) {
        const fields = [...BASES[bn].TABLES[tableName].fields]
        delete BASES[bn].TABLES[tableName].fields
        BASES[bn].TABLES[tableName].FIELDS = arr2obj(fields)
      }
    }    
    BASE = BASES[this.airtableBaseName]
    for (const [TblNm, Table] of Object.entries(BASE.TABLES)) {    
      var modelFields = {
        recid: {
          type: DataTypes.STRING,
          primaryKey: true,
        },
      }
      for (const [FldNm, Field] of Object.entries(Table.FIELDS)) {
        const typeConf = FIELD_TYPES[Field.type]
        if (this.airtableIgnoreFieldTypes.includes(Field.type)) {
          //console.log(`Ignore field type ${Field.type}`)
          continue
        }
          
        modelFields[FldNm] = {
          type: typeConf.type,
        }
      }
      console.log(`Define sqlz model: ${TblNm}`)
      MODELS[TblNm] = this.sequelize.define(TblNm, modelFields)
    }
    console.log(`All sqlz models are ready to be synced to SQL.`)


    console.log("Sync all sqlz models to SQL....")
    var syncParms = force ? {force: true} : {alter: true}
    await this.sequelize.sync(syncParms) // ❗
    console.log("...all sqlz models were synced to SQL.")
    console.log(Object.keys(this.sequelize.models))
    
    if (skipData) return

    base = this.airtable.base(BASE.id)
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

  async getMetadata(baseName) {
    if (baseName)
      return BASES[baseName]
    else
      return BASES
  }
}

// just some sample...
const BASETABLESPrices = {
  id: 'tblNIhFITOL4RVuyW',
  primaryFieldId: 'fldO89ifoSzKY8DUs',
  FIELDS: {
    ID: { type: 'formula', id: 'fldO89ifoSzKY8DUs' },
    Price: { type: 'formula', id: 'fldY7i0kwZH4zVr44' },
    Cost: { type: 'currency', id: 'fldsU8hfuZZrDr8Mq' },
    Destination: { type: 'multipleRecordLinks', id: 'fldNQg12MuGNEpT2r' },
    Service: { type: 'multipleRecordLinks', id: 'fldyW18Fbvl3sO3dY' },
    Markup: { type: 'currency', id: 'fldMQsouFmAK5EEA3' },
    Info: { type: 'multilineText', id: 'fldWj9XsNN0aibaYX' },
    Categories: { type: 'multipleLookupValues', id: 'fldY0ukiqmWsDr8my' },
    Packages: { type: 'multipleRecordLinks', id: 'fldIvUVaAjhPjYqqK' }
  }
}