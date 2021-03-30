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
var RECORDS = {}
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
  async init() {
    const basesMeta = await airtableMeta.get('bases')
    META = _.find(basesMeta.data.bases, {name: baseName})
    if (!META || !META.id) throw `Failed finding metadata for base '${baseName}'`
    const tablesMeta = await airtableMeta.get(`bases/${META.id}/tables`)
    if (!tablesMeta || !tablesMeta.data.tables) throw `Failed to get tables metadata for base '${baseName}'`
    META.tables = _.keyBy(tablesMeta.data.tables, 'name')
    for (let TblNm in META.tables) {
      META.tables[TblNm].fields = _.keyBy(META.tables[TblNm].fields, 'name')
      delete META.tables[TblNm].views
    }
    return new Promise((resolve, reject) => {      
      var tblsToFetch = new Set(Object.keys(META.tables))
      for (let TblNm in META.tables) {
        airtable.base(META.id)(TblNm).select({
          //maxRecords: 100
        }).eachPage(async function page(records, fetchNextPage) {
          for (let record of records) {
            RECS[record.id] = {
              _tableName: TblNm,
              ...record.fields
            }
            if (!RECORDS[TblNm]) RECORDS[TblNm] = {}
            RECORDS[TblNm][record.id] = RECS[record.id]
          }
          fetchNextPage()
        }, async function done(err) {
          if (err) {
            console.error(err)
            reject(err)
          } 
          setTimeout(() => {
            console.log(`Fetched all records: ${TblNm}`)
            tblsToFetch.delete(TblNm)
            if (tblsToFetch.size == 0) {
              resolve(Object.keys(RECS).length)
            }
          }, 3000);
        })
      }
    })
  }
  generateMetaData() { //returns hybrid of enriched metadata and current Airtable metadata for further enrichment by the user
    for (let TblNm in META.tables) { // save relations in META
      const table = META.tables[TblNm]
      const tblRecs = Object.values(RECORDS[TblNm])
      for (let FldNm in table.fields) {
        const field = table.fields[FldNm]
        if (field.type == 'multipleRecordLinks') {                    
          field._rel = {}
          const linkingToNone = _.filter( tblRecs, rec => (!rec[FldNm] || rec[FldNm].length == 0) ) 
          const linkingToAny  = _.filter( tblRecs, rec => (_.isArray(rec[FldNm]) && rec[FldNm].length) )
          const linkingToOne  = _.filter( linkingToAny, rec => rec[FldNm].length == 1 )
          const linkingToMany = _.filter( linkingToAny, rec => rec[FldNm].length > 1 )
          if (linkingToAny && linkingToAny[0]) {
            const recId = linkingToAny[0][FldNm][0]
            field._rel.table = RECS[recId]._tableName
            console.log(`${TblNm}.${FldNm} links to ${field._rel.table}`)
            console.log(`..${tblRecs.length} records in total`)
            console.log(`....${linkingToNone.length} records without links`)
            console.log(`....${linkingToAny.length} records with links`)
            console.log(`......${linkingToOne.length} to one record`)
            console.log(`......${linkingToMany.length} to multiple records`)
            console.log(` `)
            field._rel.linksTo = (linkingToMany && linkingToMany.length) ? 'many' : 'one'
            field._rel.mandatory = !linkingToNone || !linkingToNone.length
          } else {
            console.warn(`WARN - ${TblNm}.${FldNm} type is 'multipleRecordLinks', but none have links`)
          }
        }
      }
    }
    return META
  }
  async createSqlSchema(meta) {
    meta = meta || META
    for (let TblNm in META.tables) {
      const table = META.tables[TblNm]
      var modelFields = {
        Id: {
          type: DataTypes.STRING,
          primaryKey: true,
        },
      }
      for (let FldNm in table.fields) { // normal fields
        const field = table.fields[FldNm]
        if (fieldTypesIgnore.includes(field.type) || field.type == 'multipleRecordLinks') {
          continue
        }
        modelFields[FldNm] = { type: FIELD_TYPES[field.type].type }
      }
      MODELS[TblNm] = this.sequelize.define(TblNm, modelFields)
    }
    var refs = []
    for (let TblNm in META.tables) {
      const table = META.tables[TblNm]
      console.log('-------------------------------------------')
      for (let FldNm in table.fields) { // relations (associations)
        const field = table.fields[FldNm]
        if (field._rel) {
          const this_rel = field._rel
          const otherFK = _.find(META.tables[this_rel.table].fields, oFld => {
            return oFld._rel && oFld._rel.table == TblNm
          })
          const other_rel = META.tables[this_rel.table].fields[otherFK.name]._rel
          console.log(`${TblNm} linksTo ${this_rel.linksTo} ${this_rel.table}
              ${this_rel.table} linksTo ${other_rel.linksTo} ${TblNm} `)
      
          const ThisModel = MODELS[TblNm]
          const OtherModel = MODELS[this_rel.table]
          if (this_rel.linksTo == 'one' && other_rel.linksTo == 'one') {
            // Places 1:1 Places ...?
            // better add inheritance manually?
            if (this_rel.mandatory) {
              // ThisModel.belongsTo(OtherModel)
              // refs.push(`${TblNm}.${FldNm} belongsTo ${this_rel.table} (1:1)`)
            } else {
              // ThisModel.hasOne(OtherModel)
              // refs.push(`${TblNm}.${FldNm} hasOne ${this_rel.table} (1:1)`)              
            }
          } else if (this_rel.linksTo == 'one' && other_rel.linksTo == 'many') {
            ThisModel.belongsTo(OtherModel)
            refs.push(`${TblNm}.${FldNm} belongsTo ${this_rel.table}`)
          }/*  else if (this_rel.linksTo == 'many' && other_rel.linksTo == 'one') {
            ThisModel.hasMany(OtherModel)
            refs.push(`${TblNm}.${FldNm} hasMany ${this_rel.table}`)
          } */ else if (this_rel.linksTo == 'many' && other_rel.linksTo == 'many') {
            const nmRelTblName = [TblNm, this_rel.table].sort().join('_')
            ThisModel.belongsToMany(OtherModel, { through: nmRelTblName })
            refs.push(`${TblNm}.${FldNm} belongsToMany ${this_rel.table} through ${nmRelTblName}`)
          } else {
            throw `Cannot detect relation on ${TblNm}.${FldNm}`
          }
          console.log('-')
        }
      }
    }
    refs.map(r => console.log(r))
    await this.sequelize.sync({force: true}) // ❗
    console.log("...all sqlz models were synced to SQL.")
    console.log(Object.keys(this.sequelize.models))
    return true
  }


  async init_OLD(force = true, skipData = true) {

    
    if (skipData) return

    base = airtable.base(BASE.id)
    console.log(`Loading all data from Base id ${BASE.id}`)
    for (const [TblNm, table] of Object.entries(BASE.TABLES)) {
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
      for (const [TblNm, table] of Object.entries(BASE.TABLES)) {
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

