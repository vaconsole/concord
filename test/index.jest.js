const {init} = require('../lib/index')

test('basic', () => {
    const result = init('/tmp/test.db')
    console.log(result)
// expect(result).toEqual('')
})