#!/usr/bin/env node
const program = require('commander')
const Database = require('sqlite-async')
const { init, populate, match } = require('../lib/index')

program.command('init <dbPath>').action(async (dbPath) => {
  const db = await Database.open(dbPath)
  await init(db)
})

program
  .command('populate <dbPath> <table> <pk>')
  .action(async (dbPath, table, pk) => {
    const db = await Database.open(dbPath)
    await populate(db, table, pk)
  })

program.parse(process.argv)
