const Database = require('better-sqlite3')
const GenerateSchema = require('generate-schema')

function initJob(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS _job_meta (
            id integer NOT NULL,
            [status]  TEXT NOT NULL,
            [description] TEXT,
            [action] TEXT NOT NULL,            
            [sql] TEXT NOT NULL,
            [assign] TEXT,
            PRIMARY KEY (
                id
            )
        );
      `)
}

function jsonCreateTable(
  data = [],
  { pk = null, tableName = '_job_1', additionalColumns = {} } = {},
) {
  let SQL = `CREATE TABLE IF NOT EXISTS ${tableName} (`
  const {
    items: { properties },
  } = GenerateSchema.json('Job', data)
  const schema = Object.assign(properties, additionalColumns)
  Object.entries(schema).forEach(([key, { type, directive }], index) => {
    let sqlType = 'TEXT'
    if (type === 'string') sqlType = 'TEXT'
    if (type === 'number') sqlType = 'integer'
    if (key === pk) {
      sqlType = `${sqlType} primary key`
    }
    if (directive) {
      sqlType = `${sqlType} ${directive}`
    }
    SQL = `${SQL}${key} ${sqlType} ${
      index !== Object.entries(schema).length - 1 ? ',' : ''
    }`
  })
  return `${SQL});`
}

function insertJsonToTable(
  jsonArray = [],
  { db, tableName = 'tableName' } = {},
) {
  jsonArray.forEach((data) => {
    const values = Object.values(data)
    const SQL = `insert into ${tableName} (${Object.keys(data).join(
      ',',
    )}) values (${Object.keys(values).fill('?').join(', ')})`
    db.prepare(SQL).run(...values)
  })
}

function createJob({
  db,
  description = '',
  sql = '',
  action = 'replace',
} = {}) {
  const { lastInsertRowid } = db
    .prepare(
      'insert into _job_meta (description,sql,action, status) values (?,?,?,?) returning rowid',
    )
    .run(description, sql, action, 'init')
  const data = db.prepare(sql).all()
  const tableName = `_job_${lastInsertRowid}`
  const tableConfig = {
    tableName,
    additionalColumns: {
      _task_id: {
        type: 'number',
      },
      _status: {
        type: 'string',
        directive: "DEFAULT 'init'",
      },
    },
    pk: '_task_id',
  }
  db.exec(jsonCreateTable(data, tableConfig))
  insertJsonToTable(data, { db, tableName })
  return lastInsertRowid
}

function assignJob({ db, job_id }) {
  const tableName = `_job_${job_id}`
  const SQL = `update ${tableName} 
set _status = 'assigned'
where _status = 'init'`
  db.prepare(SQL).run()
}
function getJob(db, job_id) {
  const tableName = `_job_${job_id}`
  return db
    .prepare(`select * from ${tableName} where _status = 'assigned`)
    .all()
}

function submitJob(data, { db, job_id = 1 } = {}) {
  const tableName = `_job_${job_id}_submission`
  const tableConfig = {
    tableName,
    pk: '_task_id',
  }
  db.exec(jsonCreateTable(data, tableConfig))
  insertJsonToTable(data, { db, tableName })
}

function acceptJob({ db, job_id }) {
  // db.exec(`DROP TABLE IF EXISTS _job_${job_id};`)
  // db.exec(`alter table  _job_${job_id}_submission rename to _job_${job_id}`)
}

module.exports = {
  initJob,
  createJob,
  jsonCreateTable,
  insertJsonToTable,
  getJob,
  submitJob,
  acceptJob,
  assignJob,
}
