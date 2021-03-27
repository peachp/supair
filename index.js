const fastify = require('fastify')({ logger: false })
require('dotenv').config()
var postmark = require('postmark')
const { createClient } = require('@supabase/supabase-js')
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY)
var Supair = require('./supair.js')
var supair = new Supair({
  airtableBaseName:         'PPW_TEST',
  airtableApiKey:           process.env.AIRTABLE_API_KEY,
  airtableMetadataApiKey:   process.env.AIRTABLE_METADATA_API_KEY,
  supabaseConnectionString: process.env.SUPABASE_DB_CONN,  
})

/** Simplifications / Assumptions
 * Ignore computed fields
 * Links are awalys 1:n ...? sequelize is similar to Airtable, can deal with n:m
 * (PK column [ID] must be unique?)
 */

fastify.get('/supair/getMetaData', async (request, reply) => { // can be optional (call init directly) if don't need to enhance metadata e.g. with relations?
  return supair.getMetaData()
})
fastify.get('/supair/createSqlSchema', async (request, reply) => {
  let metaData
  if (request.body.metaData) {
    metaData = request.body.metaData // enriched meta data e.g. with mandatory fields
  } else {
    metaData = supair.getMetaData() // retrieves previously enhanced Airtable meta data or generates fresh from Airtable without any enrichments
  }
  return supair.createSqlSchema(metaData)
})
fastify.get('/supair/syncData', async (request, reply) => {
  supair.syncData(request.body)
  return `Syncing all AT data once`
  /* 
  example:
  supair.syncData({
    at2pg: 10,  // optional Integer: defaults to false. Interval in seconds how often supair will scan Airtable for new/modified records (min 5). If false, will only sync Airtable data into Postgres once (use for one-off migrations).
    pg2at: true // optional Boolean: defaults to false. Set to true to sync CUDs in Postgres to Airtable in real-time
  })
  */
})

const start = async () => {
  try {
    await fastify.register(require('fastify-express'))
    fastify.use(require('cors')())
    await fastify.listen(3002)
  } catch (err) {
    fastify.log.error(err)
    process.exit(1)
  }
}
start()