const concord = require('../lib/job')
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
  const result = concord.initJob(db)
  concord.initJob(db)
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

test('insertJsonToTable basic', () => {
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
  concord.insertJsonToTable(db, 'job_1', data)
  // db.exec(SQL)
  // db.exec(`insert into job_1 (a,b) values ('test',1)`)
  const result = db.prepare('select * from job_1').all()
  expect(result).toEqual([
    { a: 'test', b: null },
    { a: null, b: 1 },
  ])
})

test('insertJsonToTable multiple', () => {
  const data = [
    {
      a: 'test',
      b: 2,
    },
    {
      b: 1,
    },
  ]
  const SQL = concord.jsonCreateTable('job_1', data)
  db.exec(SQL)
  concord.insertJsonToTable(db, 'job_1', data)
  // db.exec(SQL)
  // db.exec(`insert into job_1 (a,b) values ('test',1)`)
  const result = db.prepare('select * from job_1').all()
  expect(result).toEqual([
    { a: 'test', b: 2 },
    { a: null, b: 1 },
  ])
})

test('createJob', () => {
  db.exec(fs.readFileSync('test/input/init_2.sql', 'utf-8'))
  concord.initJob(db)
  const job = {
    description: 'test job',
    action: 'replace',
    sql: 'select * from a',
  }
  concord.createJob(db, job)
})

test('submitJob', () => {
  db.exec(fs.readFileSync('test/input/init_2.sql', 'utf-8'))
  concord.initJob(db)
  const job = {
    description: 'test job',
    action: 'replace',
    sql: 'select * from a',
  }
  concord.createJob(db, job)
  const data = [
    { rowid: 1, id: 'a1', ref: 'b1' },
    { rowid: 2, id: 'a2', ref: 'b1' },
    { rowid: 3, id: 'a3', ref: 'c1' },
  ]
  concord.submitJob(db, 1, data)
})
