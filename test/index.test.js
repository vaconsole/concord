const concord = require('../lib/index')
const fs = require('fs')
const Database = require('sqlite-async')
const dbPath = 'test/input/input.db'

let db = null
beforeEach(async () => {
  const initSql = fs.readFileSync('test/input/init.sql', 'utf-8')
  try {
    fs.unlinkSync(dbPath)
  } catch (error) {}
  db = await Database.open(dbPath)
})

afterEach(async () => {})

test('basic_init', async () => {
  await db.exec(fs.readFileSync('test/input/init.sql', 'utf-8'))
  const result = await concord.init(db)
  await concord.init(db)
})

test('basic_populate', async () => {
  await db.exec(fs.readFileSync('test/input/init.sql', 'utf-8'))
  await concord.init(db)
  const output = await concord.populate(db, 'a', 'id')
  const result = await db.all('select * from concordance_id')
  expect(result).toEqual([
    { source: 'a', source_id: 'a1', con_id: 'a1' },
    { source: 'a', source_id: 'a2', con_id: 'a2' },
    { source: 'a', source_id: 'a3', con_id: 'a3' },
  ])
})

test('basic_populate 2', async () => {
  await db.exec(fs.readFileSync('test/input/init.sql', 'utf-8'))
  await concord.init(db)
  await concord.populate(db, 'a', 'id')
  await concord.populate(db, 'a', 'id')
  const result = await db.all('select * from concordance_id')
  expect(result).toEqual([
    { source: 'a', source_id: 'a1', con_id: 'a1' },
    { source: 'a', source_id: 'a2', con_id: 'a2' },
    { source: 'a', source_id: 'a3', con_id: 'a3' },
  ])
})

test('basic_match', async () => {
  await db.exec(fs.readFileSync('test/input/init_1.sql', 'utf-8'))
  await concord.init(db)
  await concord.populate(db, 'a', 'id')
  await concord.populate(db, 'b', 'id')
  await concord.populate(db, 'c', 'id')
  await concord.match(db, 'a', ['id', 'ref'])
  const result = await db.all('select * from concordance_id')
  expect(result).toEqual([
    { source: 'a', source_id: 'a1', con_id: 'a1' },
    { source: 'a', source_id: 'a2', con_id: 'a2' },
    { source: 'a', source_id: 'a3', con_id: 'a3' },
    { source: 'b', source_id: 'b3', con_id: 'b3' },
    { source: 'c', source_id: 'c2', con_id: 'c2' },
    { source: 'b', source_id: 'b1', con_id: 'a1' },
    { source: 'b', source_id: 'b2', con_id: 'a2' },
    { source: 'c', source_id: 'c1', con_id: 'a3' },
  ])
})

test('basic_match_2', async () => {
  await db.exec(fs.readFileSync('test/input/init_2.sql', 'utf-8'))
  await concord.init(db)
  await concord.populate(db, 'a', 'id')
  await concord.populate(db, 'b', 'id')
  await concord.populate(db, 'c', 'id')
  await concord.match(db, 'a', ['id', 'ref'])
  const result = await db.all('select * from concordance_id')
  expect(result).toEqual([
    { source: 'a', source_id: 'a1', con_id: 'a1' },
    { source: 'a', source_id: 'a2', con_id: 'a2' },
    { source: 'a', source_id: 'a3', con_id: 'a3' },
    { source: 'b', source_id: 'b2', con_id: 'b2' },
    { source: 'b', source_id: 'b3', con_id: 'b3' },
    { source: 'c', source_id: 'c2', con_id: 'c2' },
    { source: 'b', source_id: 'b1', con_id: 'a1' },
    { source: 'b', source_id: 'b1', con_id: 'a2' },
    { source: 'c', source_id: 'c1', con_id: 'a3' },
  ])
})
