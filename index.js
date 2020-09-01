const pino = require('pino')
const pull = require('pull-stream')
const connect = require('ssb-client')
const MeiliSearch = require('meilisearch')
const limit = require('call-limit')
const crypto = require('crypto')

async function main () {
  let server
  try {
    server = await connect()
    process.on('SIGINT', function() {
      console.log('Shutting down...')
      server.close()
      process.exit()
    })
  } catch (err) {
    console.error(err)
    return
  }

  const meili = new MeiliSearch({
    host: 'http://127.0.0.1:7700'
  })

  const index = await meili.getOrCreateIndex('posts', { primaryKey: '__id' })

  // pull data from sbot
  pull(
    server.query.read({}),
    pull.filter(msg => msg.value.content.type === 'post' && !msg.value.private),
    pull.drain(limit.promise(addToIndex, 30))
  )

  function createId (msg) {
    const h = crypto.createHash('sha256')
    h.update(msg.key)
    return h.digest('hex')
    return msg
  }

  function parseMsg (msg) {
    try {
      return {
        timestamp: msg.value.timestamp,
        author: msg.value.author,
        text: msg.value.content.text
      }
    } catch (e) {
      console.warn('Unable to parse msg', e)
    }
  }

  async function addToIndex (msg) {
    try {
      const parsed = parseMsg(msg)
      parsed.__id = createId(msg)
      await index.addDocuments([parsed])
      console.log(`Indexed ${parsed.__id}`)
    } catch (e) {
      console.warn('Unable to add document to index', e)
    }
  }
}

main()
