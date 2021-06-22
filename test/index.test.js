const concord = require('../lib/index')
const fs = require('fs')
const Database = require('sqlite-async')
const dbPath = 'test/input/input.db'

let db = null
beforeEach(async () => {
  const initSql = fs.readFileSync('test/input/init.sql', 'utf-8')
  db = await Database.open(':memory:')
  await db.exec(initSql)
})

test('basic_init', async () => {
  const result = await concord.init(db)
  await concord.init(db)
})

test('basic_populate', async () => {
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
  await concord.init(db)
  await concord.populate(db, 'a', 'id')
  await concord.populate(db, 'c', 'id')
  await concord.match(db, 'c')
  // const result = await db.all('select * from con_match')
  // console.log(result)
})
