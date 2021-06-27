const Database = require('better-sqlite3')
var GenerateSchema = require('generate-schema')

function initJob(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS job_meta (
            id integer NOT NULL,
            [status]  TEXT NOT NULL,
            [description] TEXT NOT NULL,
            [sql_instruction] TEXT NOT NULL,
            [assigned_to] TEXT,
            PRIMARY KEY (
                id
            )
        );
      `)
}

function jsonCreateTable(tableName = 'job_1', jsonArray = []) {
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

function createJob(
  db,
  { status = 'init', description = '', sql_instruction = '' } = {},
) {
  const { lastInsertRowid } = db
    .prepare(
      'insert into job_meta (description,sql_instruction, status) values (?,?,?) returning rowid',
    )
    .run(description, sql_instruction, status)
  console.log(lastInsertRowid)
}

module.exports = {
  initJob,
  createJob,
  jsonCreateTable,
}
