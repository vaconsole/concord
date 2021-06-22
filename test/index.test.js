const concord = require('../lib/index')
const fs = require('fs')
const Database = require('sqlite-async')
const dbPath = 'test/input/input.db'

beforeEach(async () => {
  fs.copyFile('test/input/test.db', 'test/input/input.db', (err) => {
    if (err) throw err
  })
})

test('basic_init', async () => {
  const db = await Database.open(dbPath)
  const result = await concord.init(db)
})

test('basic_populate', async () => {
  const db = await Database.open(dbPath)
  await concord.init(db)
  const output = await concord.populate(db, 'a', 'id')
  const result = await db.all('select * from con_match')
  expect(result).toEqual([
    { source: 'a', source_id: 'a1', con_id: 'a1' },
    { source: 'a', source_id: 'a2', con_id: 'a2' },
    { source: 'a', source_id: 'a3', con_id: 'a3' },
  ])
})

test('basic_populate 2', async () => {
  const db = await Database.open(dbPath)
  await concord.init(db)
  await concord.populate(db, 'a', 'id')
  await concord.populate(db, 'a', 'id')
  const result = await db.all('select * from con_match')
  expect(result).toEqual([
    { source: 'a', source_id: 'a1', con_id: 'a1' },
    { source: 'a', source_id: 'a2', con_id: 'a2' },
    { source: 'a', source_id: 'a3', con_id: 'a3' },
  ])
})

test('basic_match', async () => {
  const db = await Database.open(dbPath)
  await concord.init(db)
  await concord.populate(db, 'a', 'id')
  await concord.populate(db, 'c', 'id')
  await concord.match(db, 'c')
  // const result = await db.all('select * from con_match')
  // console.log(result)
})
