const fastify = require('fastify')({ logger: true })
require('dotenv').config()
var Supair = require('./supair.js')


var supair = new Supair({
  airtableBaseName:         'PPW_TEST',
  airtableApiKey:           process.env.AIRTABLE_API_KEY,
  airtableMetadataApiKey:   process.env.AIRTABLE_METADATA_API_KEY,
  supabaseConnectionString: process.env.SUPABASE_DB_CONN,  
})

supair.init(true, true) // uses Airtable metadata to create PostgreSQL schema and loads all data
//supair.keepInSync(3) // will wait for already running init and evtl. previous sync to finish




fastify.get('/', async (request, reply) => {
  return { hello: 'world' }
})
fastify.get('/metadata/:base', async (request, reply) => {
  const metaData = supair.getMetadata(request.params.base)
  return metaData
})
fastify.get('/metadata', async (request, reply) => {
  return supair.getMetadata()
})


// Run the server!
const start = async () => {
  try {
    await fastify.register(require('fastify-express'))
    fastify.use(require('cors')())
    await fastify.listen(3000)
  } catch (err) {
    fastify.log.error(err)
    process.exit(1)
  }
}
start()