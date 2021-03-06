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

  const SQL = concord.jsonCreateTable(data, { tableName: 'job_1' })
  db.exec(SQL)
  db.exec(`insert into job_1 (a,b) values ('test',1)`)
  const result = db.prepare('select * from job_1').all()
  expect(result).toEqual([{ a: 'test', b: 1 }])
})

test('jsonCreateTable additional Columns', () => {
  const data = [
    {
      a: 'test',
    },
    {
      b: 1,
    },
  ]
  const tableConfig = {
    tableName: 'job_1',
    additionalColumns: {
      test: {
        type: 'string',
        directive: "DEFAULT 'test'",
      },
      test1: {
        type: 'number',
      },
    },
  }
  const SQL = concord.jsonCreateTable(data, tableConfig)
  db.exec(SQL)
  db.exec(`insert into job_1 (a,b) values ('test',1)`)
  const result = db.prepare('select * from job_1').all()
  console.log(result)
  expect(result).toEqual([{ a: 'test', b: 1, test: 'test', test1: null }])
})

test('jsonCreateTable rowId', () => {
  const data = [
    {
      a: 'test',
      rowid: 1,
    },
    {
      b: 1,
      rowid: 2,
    },
  ]

  const SQL = concord.jsonCreateTable(data, { tableName: 'job_1', pk: 'rowid' })
  db.exec(SQL)
  db.exec(`insert into job_1 (a,rowid) values ('test',1)`)
  const result = db.prepare('select * from job_1').all()
  expect(result).toEqual([{ a: 'test', rowid: 1, b: null }])
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
  const tableConfig = {
    tableName: 'job_1',
  }
  const insertConfig = {
    db,
    tableName: 'job_1',
  }
  const SQL = concord.jsonCreateTable(data, tableConfig)
  db.exec(SQL)
  concord.insertJsonToTable(data, insertConfig)
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
  const tableConfig = {
    tableName: 'job_1',
  }
  const insertConfig = {
    db,
    tableName: 'job_1',
  }
  const SQL = concord.jsonCreateTable(data, tableConfig)
  db.exec(SQL)
  concord.insertJsonToTable(data, insertConfig)
  // db.exec(SQL)
  // db.exec(`insert into job_1 (a,b) values ('test',1)`)
  const result = db.prepare('select * from job_1').all()
  expect(result).toEqual([
    { a: 'test', b: 2 },
    { a: null, b: 1 },
  ])
})

test('insertJsonToTable upserts', () => {
  let data = [
    {
      a: '2',
      b: 2,
    },
    {
      a: '1',
      b: 1,
    },
  ]
  const tableConfig = {
    tableName: 'job_1',
    pk: 'b',
  }
  const insertConfig = {
    db,
    tableName: 'job_1',
    upsert: true,
  }
  const SQL = concord.jsonCreateTable(data, tableConfig)
  db.exec(SQL)
  concord.insertJsonToTable(data, insertConfig)

  data = [
    {
      a: '4',
      b: 2,
    },
    {
      a: '3',
      b: 1,
    },
    { a: '4', b: 3 },
  ]

  concord.insertJsonToTable(data, insertConfig)
  const result = db.prepare('select * from job_1').all()
  expect(result).toEqual([
    { a: '3', b: 1 },
    { a: '4', b: 2 },
    { a: '4', b: 3 },
  ])
})

test('createJob', () => {
  db.exec(fs.readFileSync('test/input/init_2.sql', 'utf-8'))
  concord.initJob(db)
  const job = {
    description: 'test job',
    pk: 'rowid',
    sql: 'select rowid,* from a',
    db,
  }
  concord.createJob(job)
  const result = db.prepare('select * from _job_1').all()
  console.log(result)
  // expect(result).toEqual([
  //   { id: 'a1', ref: 'b1', _task_id: 1, _status: 'init' },
  //   { id: 'a2', ref: 'b1', _task_id: 2, _status: 'init' },
  //   { id: 'a3', ref: 'c1', _task_id: 3, _status: 'init' },
  // ])
})

test('assignJob one', () => {
  db.exec(fs.readFileSync('test/input/init_2.sql', 'utf-8'))
  const job = {
    description: 'test job',
    pk: 'rowid',
    sql: 'select rowid,* from a',
    db,
  }
  const job_id = concord.createJob(job)
  concord.assignJob({ db, job_id })
  const result = db.prepare('select * from _job_1').all()
  console.log(result)
  // expect(result).toEqual([
  //   { id: 'a1', ref: 'b1', _task_id: 1, _status: 'assigned' },
  //   { id: 'a2', ref: 'b1', _task_id: 2, _status: 'assigned' },
  //   { id: 'a3', ref: 'c1', _task_id: 3, _status: 'assigned' },
  // ])
})

test('multiple assignJob', () => {
  db.exec(fs.readFileSync('test/input/init_2.sql', 'utf-8'))
  const job = {
    description: 'test job',
    pk: 'rowid',
    sql: 'select rowid,* from a',
    db,
  }
  const job_id = concord.createJob(job)
  concord.assignJob({ db, job_id })
  concord.assignJob({ db, job_id })
  const result = db.prepare('select * from _job_1').all()
  console.log(result)
  expect(result).toEqual([
    { rowid: 1, id: 'a1', ref: 'b1', _status: 'assigned' },
    { rowid: 2, id: 'a2', ref: 'b1', _status: 'assigned' },
    { rowid: 3, id: 'a3', ref: 'c1', _status: 'assigned' },
  ])
})

test('submitJob', () => {
  db.exec(fs.readFileSync('test/input/init_2.sql', 'utf-8'))
  concord.initJob(db)
  const job = {
    description: 'test job',
    action: 'replace',
    sql: 'select * from a',
    db,
  }
  const job_id = concord.createJob(job)
  const data = [
    { id: 'a1', ref: 'b1', _task_id: 1, _status: 'assigned' },
    { id: 'a2', ref: 'b1', _task_id: 2, _status: 'assigned' },
    { id: 'a3', ref: 'c1', _task_id: 3, _status: 'assigned' },
  ]
  const jobConfig = {
    db,
    job_id,
  }
  concord.assignJob({ db, job_id })
  concord.submitJob(data, jobConfig)
  const result = db.prepare('select * from _job_1_submission').all()
  console.log(result)
  expect(result).toEqual([
    { id: 'a1', ref: 'b1', _task_id: 1 },
    { id: 'a2', ref: 'b1', _task_id: 2 },
    { id: 'a3', ref: 'c1', _task_id: 3 },
  ])
  // expect(result).toEqual(data)
})

test('acceptJob', () => {
  db.exec(fs.readFileSync('test/input/init_2.sql', 'utf-8'))
  concord.initJob(db)
  const job = {
    description: 'test job',
    action: 'replace',
    sql: 'select * from a',
    db,
  }
  const job_id = concord.createJob(job)
  const jobConfig = {
    db,
    job_id,
  }
  concord.assignJob({ db, job_id })
  const data = [
    { id: 'a1', ref: '1', _task_id: 1, _status: 'assigned' },
    { id: 'a2', ref: '2', _task_id: 2, _status: 'assigned' },
    { id: 'a3', ref: '3', _task_id: 3, _status: 'assigned' },
  ]
  concord.submitJob(data, jobConfig)
  concord.acceptJob({ db, job_id })
  const result = db.prepare('select * from _job_1').all()
  expect(result).toEqual([
    { id: 'a1', ref: 'b1', _task_id: 1, _status: 'approved' },
    { id: 'a2', ref: 'b1', _task_id: 2, _status: 'approved' },
    { id: 'a3', ref: 'c1', _task_id: 3, _status: 'approved' },
  ])
})

test('acceptJob, simple update', () => {
  db.exec(fs.readFileSync('test/input/init_2.sql', 'utf-8'))
  concord.initJob(db)
  const job = {
    description: 'test job',
    action: 'replace',
    sql: 'select * from a',
    db,
  }
  const job_id = concord.createJob(job)
  const jobConfig = {
    db,
    job_id,
  }
  concord.assignJob({ db, job_id })
  const data = [
    { id: 'a1', ref: '1', _task_id: 1, _status: 'assigned' },
    { id: 'a2', ref: '2', _task_id: 2, _status: 'assigned' },
    { id: 'a3', ref: '3', _task_id: 3, _status: 'assigned' },
  ]
  concord.submitJob(data, jobConfig)
  concord.acceptJob({ db, job_id })

  // simple update
  const result = db
    .prepare(
      `insert into a (id,ref) Values('a4','t')
`,
    )
    .run()
  concord.assignJob({ db, job_id })
})
