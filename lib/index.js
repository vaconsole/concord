const Database = require('better-sqlite3')
const SqlString = require('sqlstring-sqlite')
var escape = require('sql-escape')

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

module.exports = { init, populate, match }
