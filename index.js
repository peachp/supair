require('dotenv').config()
const { createClient } = require('@supabase/supabase-js')
var Supair = require('./supair.js')
var supair = new Supair({
  airtableBaseName:         'PPWsupair',
  airtableApiKey:           process.env.AIRTABLE_API_KEY,
  airtableMetadataApiKey:   process.env.AIRTABLE_METADATA_API_KEY,
  supabaseConnectionString: process.env.SUPABASE_DB_CONN,  
})

/**
 * Natural or surrogate PK?
 * How to deal with n:m?
 */

async function init() {
  const totalRecs = await supair.init()
  console.log(`Basic meta set, all records fetched: ${totalRecs}`)  
  const metaData = supair.generateMetaData()
  // optionally enrich metaData e.g. with mandatory fields, onDelete: 'RESTRICT' etc.
  supair.createSqlSchema(metaData) // or pass nothing to use default meta data
}

init()