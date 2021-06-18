#!/usr/bin/env node
const program = require('commander')
const Database = require('sqlite-async')

program.command('init <dbPath> <table>').action(options => {
console.log('hello world') 
})


program.command('search <dbPath> <table>').action(options => {
console.log('hello world') 
})




program.parse(process.argv)
