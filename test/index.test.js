const concord = require('../lib/index')
const fs = require('fs')
const Database = require('better-sqlite3')

const dbPath = 'test/input/input.db'

let db = null
beforeEach(() => {
  const initSql = fs.readFileSync('test/input/init.sql', 'utf-8')
  try {
    fs.unlinkSync(dbPath)
  } catch (error) {}
  // db = new Database(dbPath, { verbose: console.log })
  db = new Database(dbPath)
})

afterEach(() => {})

test('basic_init', () => {
  db.exec(fs.readFileSync('test/input/init.sql', 'utf-8'))
  const result = concord.init(db)
  concord.init(db)
})

test('basic_populate', () => {
  db.exec(fs.readFileSync('test/input/init.sql', 'utf-8'))
  concord.init(db)
  const output = concord.populate(db, 'a', 'id')
  const result = db.prepare('select * from concordance_id')
  expect(result.all()).toEqual([
    { source: 'a', source_id: 'a1', con_id: 'a1' },
    { source: 'a', source_id: 'a2', con_id: 'a2' },
    { source: 'a', source_id: 'a3', con_id: 'a3' },
  ])
})

test('basic_populate 2', () => {
  db.exec(fs.readFileSync('test/input/init.sql', 'utf-8'))
  concord.init(db)
  concord.populate(db, 'a', 'id')
  concord.populate(db, 'a', 'id')
  const result = db.prepare('select * from concordance_id')
  expect(result.all()).toEqual([
    { source: 'a', source_id: 'a1', con_id: 'a1' },
    { source: 'a', source_id: 'a2', con_id: 'a2' },
    { source: 'a', source_id: 'a3', con_id: 'a3' },
  ])
})

test('basic_match', () => {
  db.exec(fs.readFileSync('test/input/init_1.sql', 'utf-8'))
  concord.init(db)
  concord.populate(db, 'a', 'id')
  concord.populate(db, 'b', 'id')
  concord.populate(db, 'c', 'id')
  concord.match(db, 'a', ['id', 'ref'])
  const result = db.prepare('select * from concordance_id').all()
  console.log(result)
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

test('basic_match_2', () => {
  db.exec(fs.readFileSync('test/input/init_2.sql', 'utf-8'))
  concord.init(db)
  concord.populate(db, 'a', 'id')
  concord.populate(db, 'b', 'id')
  concord.populate(db, 'c', 'id')
  concord.match(db, 'a', ['id', 'ref'])
  const result = concord.assignDeDup(db)
})

test('createJob', () => {
  db.exec(fs.readFileSync('test/input/init_2.sql', 'utf-8'))
  concord.initJob(db)
  const job = {
    status: 'init',
    description: 'test job',
    sql_instruction: 'select * from a',
  }
  concord.createJob(db, job)
  // const result = concord.assignDeDup(db)
})

test('jsonCreateTable', () => {
  const data = [
    {
      a: 'test',
    },
    {
      b: 1,
    },
  ]

  const SQL = concord.jsonCreateTable('job_1', data)
  db.exec(SQL)
  db.exec(`insert into job_1 (a,b) values ('test',1)`)
  const result = db.prepare('select * from job_1').all()
  expect(result).toEqual([{ a: 'test', b: 1 }])
})

test('assignDeDup', () => {
  db.exec(fs.readFileSync('test/input/init_2.sql', 'utf-8'))
  concord.init(db)
  concord.populate(db, 'a', 'id')
  concord.populate(db, 'b', 'id')
  concord.populate(db, 'c', 'id')
  concord.match(db, 'a', ['id', 'ref'])
  const result = db.prepare('select * from concordance_id').all()
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

test('basic_match_bug', () => {
  db.exec(fs.readFileSync('test/input/init_3.sql', 'utf-8'))
  concord.init(db)
  concord.populate(db, 'a', 'title')
  concord.populate(db, 'b', 'title')
  concord.match(db, 'a', ['title', 'ref'])
  const result = db.prepare('select * from concordance_id').all()
})

test('basic_match_bug_2', () => {
  db.exec(fs.readFileSync('test/input/init_3.sql', 'utf-8'))
  concord.init(db)
  concord.populate(db, 'a', 'title')
  concord.populate(db, 'b', 'title')
  concord.match(db, 'a', ['title', 'ref'])
  const result = db.prepare('select * from concordance_id').all()
})
