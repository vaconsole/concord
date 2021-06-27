const Database = require('better-sqlite3')
const SqlString = require('sqlstring-sqlite')
var escape = require('sql-escape')
var GenerateSchema = require('generate-schema')

function init(db) {
  try {
    db.exec(`
      CREATE TABLE concordance_id (
          source    TEXT NOT NULL,
          source_id TEXT NOT NULL,
          con_id    TEXT NOT NULL,
          PRIMARY KEY (
              source,
              source_id,
              con_id
          )
      );

      CREATE TABLE concordance_meta (
          source TEXT NOT NULL,
          [key]  TEXT NOT NULL,
          PRIMARY KEY (
              source,
              [key]
          )
      );
      
    
      `)
  } catch (error) {
    console.error(error)
  }
}

function initJob(db) {
  try {
    db.exec(`
    CREATE TABLE job_meta (
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
  } catch (error) {
    console.error(error)
  }
}

function populate(db, tableName = '', pk = '') {
  db.exec(`
      INSERT OR REPLACE INTO concordance_id (source, source_id,con_id)
      SELECT '${tableName}' source, ${pk} id ,  ${pk} con_id
      FROM  ${tableName};
      INSERT OR REPLACE INTO concordance_meta (source, key) VALUEs ('${tableName}','${pk}') ;
      `)
}

function match(db, tableName = '', indexColumns = []) {
  const ftsTableName = `_${tableName}`
  db.exec(
    `
    CREATE VIRTUAL TABLE ${ftsTableName} 
    USING FTS5(${indexColumns.join(',')});
    INSERT INTO ${ftsTableName} select ${indexColumns.join(
      ',',
    )} from ${tableName};
    `,
  )
  const { key } = db
    .prepare(`select key from concordance_meta where source = ?;`)
    .get(tableName)
  for (const row of db
    .prepare(`select rowid,* from concordance_id where source is not ?;`)
    .all(tableName)) {
    const matches = db
      .prepare(`select * from ${ftsTableName} where ${ftsTableName} match ?;`)
      .all(`"${row.con_id}"`)
    if (matches.length) {
      db.exec(`delete from concordance_id where rowid is ${row.rowid}`)
      for (const match of matches) {
        const { con_id } = db
          .prepare(
            `select * from concordance_id where source = ? and source_id = ?`,
          )
          .get(tableName, match[key])
        db.prepare(
          `INSERT OR REPLACE INTO concordance_id (source, source_id,con_id) VALUES (?,?,?)`,
        ).run(row.source, row.source_id, con_id)
      }
    }
  }
  // clean ups
  db.exec(`drop table _${tableName}`)
}

function jsonCreateTable(tableName = 'job_1', jsonArray = []) {
  let SQL = `CREATE TABLE ${tableName} (
`
  const {
    items: { properties },
  } = GenerateSchema.json('Job', jsonArray)
  console.log(JSON.stringify(properties))

  Object.entries(properties).forEach(([key, { type }], index) => {
    let sqlType = 'TEXT'
    if (type === 'string') sqlType = 'TEXT'
    if (type === 'number') sqlType = 'integer'
    SQL = `${SQL}${key} ${sqlType} ${
      index !== Object.entries(properties).length - 1 ? ',' : ''
    }`
    console.log(index)
  })
  return `${SQL}
      );`
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

function assignDeDup(db) {
  // const ftsTableName = `_${tableName}`
  initJob(db)

  db.exec(`
   CREATE TABLE job_${lastInsertRowid} (
          id integer NOT NULL,
          [source]  TEXT NOT NULL,
          [source_id] TEXT NOT NULL,
          [match_source] TEXT NOT NULL,
          [match_source_id] TEXT,
          PRIMARY KEY (
              id
          )
      );
  `)
  const result = db
    .prepare(
      `
      INSERT INTO job_${lastInsertRowid}
      SELECT 
            NULL ROWID,
            [source:1] source,
            [source_id:1] source_id,
            source match_source,
            source_id match_source_id
        FROM (
                SELECT *
                  FROM concordance_id
                        JOIN
                        (
                            SELECT concordance_id.*
                              FROM concordance_id
                                  JOIN
                                  (
                                      SELECT source,
                                              source_id
                                        FROM concordance_id
                                        GROUP BY source_id
                                      HAVING count( * ) > 1
                                  )
                                  dup_cons ON concordance_id.source = dup_cons.source AND 
                                              concordance_id.source_id = dup_cons.source_id
                        )
                        dups ON concordance_id.con_id = dups.con_id AND 
                                concordance_id.source <> dups.source
            );
          `,
    )
    .run()

  console.log(result)
  // db.exec(
  //   `
  //   CREATE VIRTUAL TABLE ${ftsTableName}
  //   USING FTS5(${indexColumns.join(',')});
  //   INSERT INTO ${ftsTableName} select ${indexColumns.join(
  //     ',',
  //   )} from ${tableName};
  //   `,
  // )
  // const { key } = db
  //   .prepare(`select key from concordance_meta where source = ?;`)
  //   .get(tableName)
  // for (const row of db
  //   .prepare(`select rowid,* from concordance_id where source is not ?;`)
  //   .all(tableName)) {
  //   const matches = db
  //     .prepare(`select * from ${ftsTableName} where ${ftsTableName} match ?;`)
  //     .all(`"${row.con_id}"`)
  //   if (matches.length) {
  //     db.exec(`delete from concordance_id where rowid is ${row.rowid}`)
  //     for (const match of matches) {
  //       const { con_id } = db
  //         .prepare(
  //           `select * from concordance_id where source = ? and source_id = ?`,
  //         )
  //         .get(tableName, match[key])
  //       db.prepare(
  //         `INSERT OR REPLACE INTO concordance_id (source, source_id,con_id) VALUES (?,?,?)`,
  //       ).run(row.source, row.source_id, con_id)
  //     }
  //   }
  // }
  // clean ups
}

module.exports = {
  init,
  populate,
  match,
  assignDeDup,
  initJob,
  createJob,
  jsonCreateTable,
}
