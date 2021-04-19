require('dotenv').config()
const { createClient } = require('@supabase/supabase-js')
var Supair = require('./supair.js')
var supair = new Supair({
  airtableBaseName:         'PPWsupair',
  airtableApiKey:           process.env.AIRTABLE_API_KEY,
  airtableMetadataApiKey:   process.env.AIRTABLE_METADATA_API_KEY,
  supabaseConnectionString: process.env.SUPABASE_DB_CONN,  
})


async function init() {
  await supair.init()
  await supair.generateMetaData()
  await supair.createBasicSchema()
  await supair.insertData()
  await supair.addConstraints()
  await supair.test()
}

init()