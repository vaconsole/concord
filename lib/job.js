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

function jsonCreateTable(tableName = '_job_1', jsonArray = []) {
  let SQL = `CREATE TABLE ${tableName} (`
  const {
    items: { properties },
  } = GenerateSchema.json('Job', jsonArray)
  Object.entries(properties).forEach(([key, { type }], index) => {
    let sqlType = 'TEXT'
    if (type === 'string') sqlType = 'TEXT'
    if (type === 'number') sqlType = 'integer'
    SQL = `${SQL}${key} ${sqlType} ${
      index !== Object.entries(properties).length - 1 ? ',' : ''
    }`
  })
  return `${SQL});`
}

function insertJsonToTable(db, tableName = 'tableName', jsonArray = []) {
  jsonArray.forEach((data) => {
    const values = Object.values(data)
    const SQL = `insert into ${tableName} (${Object.keys(data).join(
      ',',
    )}) values (${Object.keys(values).fill('?').join(', ')})`
    db.prepare(SQL).run(...values)
  })
}

function createJob(
  db,
  { description = '', sql = '', action = 'replace' } = {},
) {
  const { lastInsertRowid } = db
    .prepare(
      'insert into _job_meta (description,sql,action, status) values (?,?,?,?) returning rowid',
    )
    .run(description, sql, action, 'init')
  const data = db.prepare(sql).all()
  db.exec(jsonCreateTable(`_job_${lastInsertRowid}`, data))
  db.exec(`
  insert into _job_${lastInsertRowid}
  ${sql}`)
}

function getJob(db, job_id) {
  const tableName = `_job_${job_id}`
  return db.prepare(`select rowid,* from ${tableName}`).all()
}

function submitJob(db, job_id, data) {
  const tableName = `_job_${job_id}_submission`
  db.exec(jsonCreateTable(tableName, data))
  // db.exec(`
  // insert into ${tableName}
  // ${sql}`)
}

module.exports = {
  initJob,
  createJob,
  jsonCreateTable,
  insertJsonToTable,
  getJob,
  submitJob,
}
