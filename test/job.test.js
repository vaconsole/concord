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

test('createJob', () => {
  db.exec(fs.readFileSync('test/input/init_2.sql', 'utf-8'))
  concord.initJob(db)
  const job = {
    status: 'init',
    description: 'test job',
    action: 'upsert|replace',
    sql_instruction: 'select * from a',
  }
  concord.createJob(db, job)
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
