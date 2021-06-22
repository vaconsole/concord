const Database = require('sqlite-async')

async function init(db) {
  try {
    await db.run(
      `CREATE TABLE con_match(
    source text NOT NULL,
    source_id text NOT NULL,
    con_id text NOT NULL,
    PRIMARY KEY(source,source_id,con_id)
      )`,
    )
  } catch (error) {
    console.error(error)
  }
}

async function populate(db, tableName = '', pk = '') {
  const SQL = `INSERT OR REPLACE INTO con_match (source, source_id,con_id)
      SELECT '${tableName}' source, id ${pk}, id ${pk}
      FROM  ${tableName} `
  return db.all(SQL)
}

async function match(db, tableName = '') {
  // const SQL = `INSERT OR REPLACE INTO con_match (source, source_id,con_id)
  //     SELECT '${tableName}' source, id ${pk}, id ${pk}
  //     FROM  ${tableName} `
  // console.log(SQL)
  const result = await db.all(
    `select rowid,* from con_match where source = '${tableName}' `,
  )

  for (const row of result) {
    console.log(row)
    let SQL = `select * from con_match where source is not '${tableName}' and con_id is '${row.con_id}'`
    const matches = await db.all(SQL)
    if (matches.length) {
      SQL = `delete * from con_match where rowid is ${row.rowid}`
      console.log(SQL)
    }
    for (const match of matches) {
      SQL = `INSERT OR REPLACE INTO con_match (source, source_id,con_id) VALUES ('${row.source}','${row.source_id}','${match.con_id}')`
      console.log(SQL)
    }
  }

  // console.log(result)
  // // result = await db.all('select * from b')
  // // console.log(result)
  // await db.close()
}

module.exports = { init, populate, match }
