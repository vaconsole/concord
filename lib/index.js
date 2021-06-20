const Database = require('sqlite-async')

async function init(dbPath = '') {
  const db = await Database.open('./test/input/input.db')
  //   await db.run("CREATE TABLE lorem (info TEXT)")
  //  var stmt = await db.prepare("INSERT INTO lorem VALUES (?)");
  //   for (var i = 0; i < 10; i++) {
  //       await stmt.run("Ipsum " + i);
  //   }
  //   await stmt.finalize();
  // let result = await db.all("select * from a");
  // console.log(result);
  // result = await db.all('select * from b')
  // console.log(result)

  await db.run(
    `CREATE TABLE con_match(
    source text NOT NULL,
    source_id text NOT NULL,
    con_id text NOT NULL,
    PRIMARY KEY(source,source_id)
      )`,
  )
  await db.close()
}

async function populate(dbPath = '', tableName = '', pk = '') {
  const db = await Database.open('./test/input/input.db')
  const SQL = `INSERT OR REPLACE INTO con_match (source, source_id,con_id)
SELECT '${tableName}' source, id ${pk}, id ${pk}
FROM  ${tableName} `
  console.log(SQL)
  const result = await db.all(SQL)
  console.log(result)
  // result = await db.all('select * from b')
  // console.log(result)
  await db.close()
}

module.exports = { init, populate }
