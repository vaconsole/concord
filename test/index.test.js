const concord = require('../lib/index')
const fs = require('fs')

beforeEach(() => {
  fs.copyFile('test/input/test.db', 'test/input/input.db', (err) => {
    if (err) throw err
    console.log('=============')
  })
})

test('basic_init', async () => {
  const result = await concord.init('/tmp/test.db')
  console.log(result)
  // expect(result).toEqual('')
})

test('basic_populate', async () => {
  await concord.init('/tmp/test.db')
  const result = await concord.populate('/tmp/test.db', 'a', 'id')
  await concord.populate('/tmp/test.db', 'b', 'id')
  console.log(result)
  // expect(result).toEqual('')
})
