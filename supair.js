require('dotenv').config()
var _ = require('lodash')
const axios = require('axios').default
const { snakeCase } = require('change-case')
var Airtable = require('airtable');
const EventEmitter = require('events')
const { createClient } = require('@supabase/supabase-js')
const { Client } = require('pg')
const pg = new Client({
  connectionString: process.env.SUPABASE_DB_CONN,
})
function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

var supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY)

class Emitter extends EventEmitter {}
const EVENTS = new Emitter()
WIP = {}

var airtableMeta
var airtable
var baseName
var META
var FIELD_TYPES_IGNORE = ['multipleLookupValues', 'rollup', 'lastModifiedTime']
var BASES
var BASE = {}
var MODELS = {}
var RECS = {/** rec123sdfkj: {foo: 'bar'} */}
var RECORDS = {}
const FIELD_TYPES = {
  autoNumber:			        'INTEGER',
  barcode:			          'JSON',
  button:			            'JSON',
  checkbox:			          'BOOLEAN',
  count:			            'INTEGER',
  createdBy:			        'JSON',
  createdTime:			      'TIMESTAMPTZ',
  currency:			          'NUMERIC(12,2)',
  date:			              'DATE',
  dateTime:			          'TIMESTAMPTZ',
  duration:			          'INTEGER',
  email:			            'TEXT',
  formula:			          'TEXT',
  lastModifiedBy:			    'JSON',
  lastModifiedTime:	      'TIMESTAMPTZ',
  multilineText:			    'TEXT',
  multipleAttachments:    'JSON',
  multipleCollaborators:	'JSON',
  multipleLookupValues:	  'JSON',
  multipleRecordLinks:    'TEXT',
  multipleSelects:	      'TEXT []',
  number:			            'NUMERIC(12,2)',
  percent:			          'NUMERIC(12,2)',
  phoneNumber:			      'TEXT',
  rating:			            'INTEGER',
  richText:			          'TEXT',
  rollup:			            'TEXT',
  singleCollaborator:		  'JSON',
  singleLineText:			    'TEXT',
  singleSelect:			      'TEXT',
  url:			              'TEXT',
}
var SQL = {
  createTables: {
    /* TableA: {
        fields: []
    } */
  },
  createTablesNM: {
    /* TableA: {
        fields: []
    } */
  },
  addFKs: [],
}
var nmRecs = {}

var lastRefresh
var base

module.exports = class Supair {
  constructor({
    supabaseConnectionString,
    airtableApiKey,
    airtableMetadataApiKey,
    airtableBaseName
  }) {    
    baseName = airtableBaseName
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
  }
  async init() {    
    await pg.connect()
    const basesMeta = await airtableMeta.get('bases')
    META = _.find(basesMeta.data.bases, {name: baseName})
    if (!META || !META.id) throw `Failed finding metadata for base '${baseName}'`
    const tablesMeta = await airtableMeta.get(`bases/${META.id}/tables`)
    if (!tablesMeta || !tablesMeta.data.tables) throw `Failed to get tables metadata for base '${baseName}'`
    META.tables = _.keyBy(tablesMeta.data.tables, 'name')
    for (let TblNm in META.tables) {
      const allFieldsArr = _.cloneDeep(META.tables[TblNm].fields)
      META.tables[TblNm].fields = {}
      for (let field of allFieldsArr) {
        if (field.id == META.tables[TblNm].primaryFieldId || !FIELD_TYPES_IGNORE.includes(field.type)) {
          META.tables[TblNm].fields[field.name] = field
        }
      }
      delete META.tables[TblNm].views
    }
    return new Promise((resolve, reject) => {      
      var tblsToFetch = new Set(Object.keys(META.tables))
      for (let TblNm in META.tables) {
        airtable.base(META.id)(TblNm).select({
          //maxRecords: 100
        }).eachPage(async function page(records, fetchNextPage) {
          console.log(`...fetched ${records.length} ${TblNm}`)
          for (let record of records) {
            RECS[record.id] = {
              id: record.id,
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
            console.log(`Fetched all: ${TblNm}`)
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
    for (let TblNm in META.tables) { // ! run full loop because links will be examined on both sides later
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
            if (!recId) console.error(`no recId`, linkingToAny[0][FldNm])
            if (!RECS[recId]) console.error(`no REC with recId ${recId} (used as link in field ${FldNm} in:)`, linkingToAny[0])
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
            console.warn(`WARN - ${TblNm}.${FldNm} type is 'multipleRecordLinks', but failed to guess the relation because the field is empty`)
          }
        }
      }
    }
    return META
  }
  validateData() {
    // first (PK) column is actually unique & not empty etc.
  }
  async createBasicSchema(meta) {
    meta = meta || META
    function otherRel(TblNm, FldNm) {
      const this_rel = META.tables[TblNm].fields[FldNm]._rel
      const otherFK = _.find(META.tables[this_rel.table].fields, otherFld => {
        return otherFld._rel && otherFld._rel.table == TblNm
      })
      return META.tables[this_rel.table].fields[otherFK.name]._rel
    }
    for (let TblNm in META.tables) {
      const table = META.tables[TblNm]
      SQL.createTables[TblNm] = {
        fields: []
      }
      SQL.createTables[TblNm].fields.push(`"id" TEXT PRIMARY KEY`)
      for (let FldNm in table.fields) {
        const field = table.fields[FldNm]
        if (field.type == 'multipleRecordLinks') {
          const this_rel = field._rel
          const other_rel = otherRel(TblNm, FldNm)
          if (this_rel.linksTo == 'one' && other_rel.linksTo == 'one') {
            // Places self ref Places...?
            if (this_rel.mandatory) {
              this_rel.link = '1:1'              
              SQL.createTables[TblNm].fields.push(`"${FldNm}" TEXT`)
              SQL.addFKs.push(`ALTER TABLE ONLY "${TblNm}" ADD CONSTRAINT "fk_${this_rel.table}_${FldNm}" FOREIGN KEY("${FldNm}") REFERENCES "${this_rel.table}"("id") ON DELETE SET NULL;`)
              SQL.addFKs.push(`ALTER TABLE ONLY "${TblNm}" ALTER COLUMN "${FldNm}" SET NOT NULL;`)              
            } else {
              // ?
            }
          } else if (this_rel.linksTo == 'one' && other_rel.linksTo == 'many') {
            this_rel.link = 'n:1'
            SQL.createTables[TblNm].fields.push(`"${FldNm}" TEXT`)
            SQL.addFKs.push(`ALTER TABLE ONLY "${TblNm}" ADD CONSTRAINT "fk_${this_rel.table}_${FldNm}" FOREIGN KEY("${FldNm}") REFERENCES "${this_rel.table}"("id") ON DELETE SET NULL;`)
          } else if (this_rel.linksTo == 'many' && other_rel.linksTo == 'one') {
            this_rel.link = '1:n'
            //delete META.tables[TblNm].fields[FldNm]
            // ...it's an Airtable thing to have FK on both sides...
          } else if (this_rel.linksTo == 'many' && other_rel.linksTo == 'many') {
            this_rel.link = 'n:m'
            const nmTblNm = [TblNm, this_rel.table].sort().join('_')
            this_rel.nmTblNm = nmTblNm
            this_rel.fldA = `${TblNm}_id`
            this_rel.fldB = `${this_rel.table}_id`
            if (!SQL.createTablesNM[nmTblNm]) {
              SQL.createTablesNM[nmTblNm] = `CREATE TABLE IF NOT EXISTS "${nmTblNm}" (
                "${this_rel.fldA}" TEXT,
                "${this_rel.fldB}" TEXT,
                PRIMARY KEY ("${this_rel.fldA}", "${this_rel.fldB}") );
              `
              SQL.addFKs.push(`ALTER TABLE ONLY "${nmTblNm}" ADD CONSTRAINT "fk_${TblNm}" FOREIGN KEY("${this_rel.fldA}") REFERENCES "${TblNm}"("id") ON DELETE SET NULL;`)
              SQL.addFKs.push(`ALTER TABLE ONLY "${nmTblNm}" ADD CONSTRAINT "fk_${this_rel.table}" FOREIGN KEY("${this_rel.fldB}") REFERENCES "${this_rel.table}"("id") ON DELETE SET NULL;`)              
            }
          } else {
            console.warn(`Cannot detect relation on ${TblNm}.${FldNm}; this_rel, other_rel:`)
            console.log(this_rel)
            console.log(other_rel)            
          }
          console.log(`${TblNm}.${FldNm} ---${this_rel.link}--> ${this_rel.table}`)
          console.log(' ')                  
        } else {
          SQL.createTables[TblNm].fields.push(`"${FldNm}" ${FIELD_TYPES[field.type]}`)
        }                
      }
    }
    for (let TblNm in SQL.createTables) {
      const delres = await pg.query(`DROP TABLE IF EXISTS "${TblNm}" CASCADE`)
      console.log(delres.command ? `${delres.command} ${TblNm}` : 'Error in DROP query')
      await sleep(200)
      const Q = `CREATE TABLE IF NOT EXISTS "${TblNm}" (\n${SQL.createTables[TblNm].fields.join(',\n')} );`
      //console.log(Q)
      const cres = await pg.query(Q)
      console.log(cres.command ? `${cres.command} ${TblNm}` : 'Error in CREATE query')
      await sleep(200)
    }
    for (let nmTblNm in SQL.createTablesNM) {
      const delres = await pg.query(`DROP TABLE IF EXISTS "${nmTblNm}" CASCADE`)
      console.log(delres.command ? `${delres.command} ${nmTblNm}` : 'Error in DROP query')
      await sleep(200)
      const cres = await pg.query(SQL.createTablesNM[nmTblNm])
      console.log(cres.command ? `${cres.command} ${nmTblNm}` : 'Error in CREATE query')
      await sleep(200)
    }

    return
  }
  async insertData(params) {    
    for (let TblNm in RECORDS) {
      let chunks = _.chunk(Object.values(RECORDS[TblNm]), 100)
      //console.log(`########## ${chunks.length} chunks of 100 ${TblNm} records`)
      for (let chunk of chunks) {
        const sqlRecs = chunk.map(rec => {
          let sqlRec = { id: rec.id }
          const TblNm = rec._tableName
          delete rec._tableName
          delete rec.id
          for (let FldNm in META.tables[TblNm].fields) {
            const field = META.tables[TblNm].fields[FldNm]
            if (field.type == 'multipleRecordLinks') {
              //if (TblNm == 'Categories') console.log(`multipleRecordLinks ${TblNm}.${FldNm}:`, field._rel)
              if (field._rel.link == 'n:1' || field._rel.link == '1:1') {
                sqlRec[FldNm] = rec[FldNm] ? rec[FldNm][0] : null
              } else if (field._rel.link == 'n:m' && rec[FldNm]) {
                const nmTblNm = field._rel.nmTblNm
                if (!nmRecs[nmTblNm]) nmRecs[nmTblNm] = []
                for (let recId of rec[FldNm]) {
                  var nmRec = {}
                  nmRec[field._rel.fldA] = sqlRec.id
                  nmRec[field._rel.fldB] = recId
                  const exists = _.find(nmRecs[nmTblNm], rec => rec[field._rel.fldA] == sqlRec.id && rec[field._rel.fldB] == recId)
                  if(!exists) nmRecs[nmTblNm].push(nmRec)
                }
              }
            } else {
              sqlRec[FldNm] = rec[FldNm] ? rec[FldNm] : null
            }
          }
          return sqlRec
        })
        //const tstRec = _.find(sqlRecs, {ID: 'Kalyn & Dana - Videography 4hrs'})
        //if (!tstRec) continue
        //console.log( tstRec )
        //if (!(TblNm == 'Events' || TblNm == 'Categories' || TblNm == 'Places')) continue // TESTING
        const res = await supabase
          .from(TblNm)
          .insert(sqlRecs)
        
        if (res.data) {
          console.log(`inserted ${sqlRecs.length} ${TblNm}:`)
          //console.log(res.data.map(row => row.id))
        } else {
          console.log(`error while inserting ${TblNm}:`)
          console.log(res.error)
          console.log(sqlRecs)
          return
        }
        
      }
    }
    for (let TblNm in nmRecs) {
      const resNM = await supabase
        .from(TblNm)
        .insert(nmRecs[TblNm])
      if (resNM.data) {
        console.log(`inserted ${TblNm}:`)
        //console.log(resNM.data.map(row => row.id))
      } else {
        console.log(`error while inserting ${TblNm}:`)
        console.log(resNM.error)
        return
      }
    }
    //pg.end()
  }
  async addConstraints() {
    for (let constr of SQL.addFKs) {
      const res = await pg.query(constr)
      console.log(res.command ? `${constr}` : 'Error adding contraint query')
      await sleep(200)
    }
  }
  async test() {
    const { data, error } = await supabase
    .from('EventDetails')
    .select(`
      ID,
      Event (
        ID
      )
    `)
    console.log(data)
  }
}