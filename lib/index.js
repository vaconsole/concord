const Database = require('sqlite-async')

async function init(db) {
  try {
    await db.run(
      `CREATE TABLE concordance_id(
    source text NOT NULL,
    source_id text NOT NULL,
    con_id text NOT NULL,
    PRIMARY KEY(source,source_id,con_id)
      )`,
    )

    await db.run(
      `CREATE TABLE concordance_meta(
    source text NOT NULL,
    key text NOT NULL,
    PRIMARY KEY(source,key)
      )`,
    )
  } catch (error) {
    console.error(error)
  }
}

async function populate(db, tableName = '', pk = '') {
  const SQL = `INSERT OR REPLACE INTO concordance_id (source, source_id,con_id)
      SELECT '${tableName}' source, id ${pk}, id ${pk}
      FROM  ${tableName};`
  await db.all(SQL)
  await db.all(
    `INSERT OR REPLACE INTO concordance_meta (source, key) VALUEs ('${tableName}','${pk}') ;`,
  )
}

async function match(db, tableName = '', indexColumns = []) {
  let SQL = `CREATE VIRTUAL TABLE _${tableName} 
    USING FTS5(${indexColumns.join(',')});
    `
  await db.all(SQL)

  await db.all(
    `INSERT INTO _${tableName} select ${indexColumns.join(
      ',',
    )} from ${tableName};`,
  )

  const [{ key }] = await db.all(
    `select key from concordance_meta where source = '${tableName}'`,
  )
  const result = await db.all(
    `select rowid,* from concordance_id where source is not '${tableName}' `,
  )
  for (const row of result) {
    const matches = await db.all(
      `select * from _${tableName} where _${tableName} match '${row.con_id}'`,
    )
    if (matches.length) {
      await db.all(`delete from concordance_id where rowid is ${row.rowid}`)
      for (const match of matches) {
        const [{ con_id }] = await db.all(
          `select * from concordance_id where source = '${tableName}' and source_id = '${match[key]}'`,
        )
        await db.all(
          `INSERT OR REPLACE INTO concordance_id (source, source_id,con_id) VALUES ('${row.source}','${row.source_id}','${con_id}')`,
        )
      }
    }
  }
  // clean ups
  await db.run(`drop table _${tableName}`)
}

module.exports = { init, populate, match }
