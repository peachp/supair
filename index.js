const fastify = require('fastify')({ logger: true })
require('dotenv').config()
var imaps = require('imap-simple')
const addrparser = require('address-rfc2822')
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

process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0"

async function recordEmails() {
  var config = {
    imap: {
      user: process.env.IMAP_USER,
      password: process.env.IMAP_PASS,
      host: 'imap.gmail.com',
      port: 993,
      tls: true,
      authTimeout: 5000
    }
  }
  console.log(`...Look for emails in ${config.imap.user} to record emails in DB`)
  imaps.connect(config).then(connection => {
    return connection.openBox('INBOX')
      .then(() => connection.search(['ALL'], { bodies: ['HEADER'] }))
      .then(async function(messages) {        
        for (let message of messages) {
          //console.log('-------------- message.parts[0].body -----------------')
          //console.log(message.parts[0].body)
          //console.log('-------------- message.attributes -----------------')
          //console.log(message.attributes)          
          try {
            const body = message.parts[0].body
            var record_v2 = {}
            var issues = []
            record_v2.msgid          = message.attributes['x-gm-msgid']
            record_v2.date           = message.attributes.date
            record_v2.subject        = body.subject ? body.subject[0] : null
            record_v2.auto_submitted = body['auto-submitted'] ? body['auto-submitted'][0] : null
            if (body.from) {
              var from
              try {
                from = addrparser.parse(body.from[0])
                record_v2.from = from[0].address
              } catch (error) {
                issues.push(`couldn't parse 'from': ${record_v2.msgid} ${record_v2.date} ${record_v2.subject}`)
              }
            } else {
              record_v2.from = null
              issues.push(`field 'from' is falsy: ${record_v2.msgid} ${record_v2.date} ${record_v2.subject}`)
            }
            if (body.to) {
              var to
              try {
                to = addrparser.parse(body.to[0])
                record_v2.to = to.map(f => f.address)
              } catch (error) {
                issues.push(`couldn't parse 'to': ${record_v2.msgid} ${record_v2.date} ${record_v2.subject}`)
              }
            } else {
              record_v2.to = null
              issues.push(`field 'to' is falsy: ${record_v2.msgid} ${record_v2.date} ${record_v2.subject}`)
            }   
            if (body.cc) {
              var cc
              try {
                cc = addrparser.parse(body.cc[0])
                record_v2.cc = cc.map(f => f.address)
              } catch (error) {
                issues.push(`couldn't parse 'cc': ${record_v2.msgid} ${record_v2.date} ${record_v2.subject}`)
              }
            } else {
              record_v2.cc = null
            }

            if (record_v2.subject.includes('TESTEMAIL')) {
              issues.push(`Test email...`)
              issues.push(JSON.stringify(record_v2, null, ' '))
            }            
            if (issues.length) {
              var emailClient = new postmark.ServerClient(process.env.POSTMARK_KEY);
              emailClient.sendEmail({
                "From": "bot@peachperfect.eu",
                "To": "waldemar@pross.ie",
                "Subject": "Email parsing issue",
                "HtmlBody": issues.join('<br />'),
                "MessageStream": "outbound"
              });
            }

            // TODO upsert by msgid in case failed to delete (conflict column, Steve's comment)
            const { data: inserted, error } = await supabase.from('email_log_v2').insert([record_v2])
            if (!error && inserted && inserted.length) {
              console.log(`email inserted in Supabase, deleting from inbox`)
              console.log(inserted)             
              connection.addFlags(message.attributes.uid, "\Deleted", (err) => {
                if (err) {
                  console.log('Problem flagging message');
                }
              })
            } else {
              console.log(`error inserting into DB`)
              console.log(error)
            }
          } catch (error) {
            console.error(error)
          }
        }
        connection.end()
        return true
      });
  })
}

async function init() {
  setInterval(recordEmails, 10000)

  supair.init(true, true) // uses Airtable metadata to create PostgreSQL schema and loads all data
  //supair.keepInSync(3) // will wait for already running init and evtl. previous sync to finish
}

init()



fastify.get('/supair', async (request, reply) => {
  return { hello: 'world' }
})
fastify.get('/supair/metadata/:base', async (request, reply) => {
  const metaData = supair.getMetadata(request.params.base)
  return metaData
})
fastify.get('/supair/metadata', async (request, reply) => {
  return supair.getMetadata()
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