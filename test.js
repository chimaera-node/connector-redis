const _ = require('rubico')
const a = require('a-sert')
const exception = require('@chimaera/exception')
const connectorRedis = require('.')

describe('connectorRedis', () => {
  it('establishes a redis string connection', () => _.flow(
    connectorRedis,
    a.eq(_.get('uri'), 'chimaera://[redis:string:local]/my_string'),
    a.eq(_.get('prefix'), 'my_string'),
    a.eq(_.get('connection_options.host', 'localhost')),
    a.eq(_.get('connection_options.port', 6379)),
    _.effect(conn => conn.free()),
    _.effect(conn => a.err(conn.ready, exception.ConnectionNotReady(conn.uri))()),
    _.effect(conn => conn.init()),
    _.effect(conn => conn.init()),
    _.effect(conn => conn.ready()),
    _.effect(conn => conn.put({ _id: '1', _source: 'hey' })),
    _.effect(conn => _.flow(
      a.eq(conn.get, { _id: '1', _source: 'hey' }),
    )('1')),
    _.effect(conn => conn.update({ _id: '1', _source: 'ho' })),
    _.effect(conn => _.flow(
      a.eq(conn.get, { _id: '1', _source: 'ho' }),
    )('1')),
    _.effect(conn => conn.delete('1')),
    _.effect(conn => a.err(
      conn.get,
      exception.ResourceNotFound(`${conn.uri}/1`),
    )('1')),
    _.effect(conn => conn.free()),
    _.effect(conn => conn.client.quit()),
  )({
    connector: 'redis',
    datatype: 'string',
    env: 'local',
    prefix: 'my_string',
    endpoint: 'redis://localhost:6379',
  })).timeout(2 * 60 * 1000)

  it('establishes a redis set connection', () => _.flow(
    connectorRedis,
    a.eq(_.get('uri'), 'chimaera://[redis:set:local]/my_set'),
    a.eq(_.get('prefix'), 'my_set'),
    a.eq(_.get('connection_options.host', 'localhost')),
    a.eq(_.get('connection_options.port', 6379)),
    _.effect(conn => conn.free()),
    _.effect(conn => a.err(conn.ready, exception.ConnectionNotReady(conn.uri))()),
    _.effect(conn => conn.init()),
    _.effect(conn => conn.init()),
    _.effect(conn => conn.ready()),
    _.effect(conn => conn.put({
      _id: '1',
      _source: ['hey'],
    })),
    _.effect(conn => a.eq(conn.get, {
      _id: '1',
      _source: ['hey'],
    })('1')),
    _.effect(conn => conn.update({
      _id: '1',
      _source: ['ho', 'hi'],
    })),
    _.effect(conn => _.flow(
      conn.get,
      a.eq(_.get('_id', '1')),
      a.eq(_.flow(_.get('_source'), _.size), 3),
      a.ok(_.flow(
        _.get('_source'),
        _.and(_.has('hey'), _.has('ho'), _.has('hi')),
      )),
    )('1')),
    _.effect(conn => conn.delete('1')),
    _.effect(conn => a.err(
      conn.get,
      exception.ResourceNotFound(`${conn.uri}/1`),
    )('1')),
    _.effect(conn => conn.free()),
    _.effect(conn => conn.client.quit()),
  )({
    connector: 'redis',
    datatype: 'set',
    env: 'local',
    prefix: 'my_set',
    endpoint: 'redis://localhost:6379',
  })).timeout(2 * 60 * 1000)

  it('establishes a redis sorted_set connection', () => _.flow(
    connectorRedis,
    a.eq(_.get('uri'), 'chimaera://[redis:sorted_set:local]/my_sorted_set'),
    a.eq(_.get('prefix'), 'my_sorted_set'),
    a.eq(_.get('connection_options.host', 'localhost')),
    a.eq(_.get('connection_options.port', 6379)),
    _.effect(conn => conn.free()),
    _.effect(conn => a.err(conn.ready, exception.ConnectionNotReady(conn.uri))()),
    _.effect(conn => conn.init()),
    _.effect(conn => conn.init()),
    _.effect(conn => conn.ready()),
    _.effect(conn => conn.put({
      _id: '1',
      _source: [1, 'hey'],
    })),
    _.effect(conn => a.eq(conn.get, {
      _id: '1',
      _source: ['hey'],
    })('1')),
    _.effect(conn => a.eq(conn.get, {
      _id: '1',
      _source: ['hey', '1'],
    })({ _id: '1', with_scores: true })),
    _.effect(conn => conn.update({
      _id: '1',
      _source: ['2', 'ho', '3', 'hi'],
    })),
    _.effect(conn => a.eq(conn.get, {
      _id: '1',
      _source: ['hey', 'ho', 'hi'],
    })('1')),
    _.effect(conn => a.eq(conn.get, {
      _id: '1',
      _source: ['hey', '1', 'ho', '2', 'hi', '3'],
    })({ _id: '1', with_scores: true })),
    _.effect(conn => conn.delete('1')),
    _.effect(conn => a.err(
      conn.get,
      exception.ResourceNotFound(`${conn.uri}/1`),
    )('1')),
    _.effect(conn => conn.free()),
    _.effect(conn => conn.client.quit()),
  )({
    connector: 'redis',
    datatype: 'sorted_set',
    env: 'local',
    prefix: 'my_sorted_set',
    endpoint: 'redis://localhost:6379',
  })).timeout(2 * 60 * 1000)

  it('establishes a redis hash connection', () => _.flow(
    connectorRedis,
    a.eq(_.get('uri'), 'chimaera://[redis:hash:local]/my_hash'),
    a.eq(_.get('prefix'), 'my_hash'),
    a.eq(_.get('connection_options.host', 'localhost')),
    a.eq(_.get('connection_options.port', 6379)),
    _.effect(conn => conn.free()),
    _.effect(conn => a.err(conn.ready, exception.ConnectionNotReady(conn.uri))()),
    _.effect(conn => conn.init()),
    _.effect(conn => conn.init()),
    _.effect(conn => conn.ready()),
    _.effect(conn => conn.put({
      _id: '1',
      _source: { a: '1', b: 'yo' },
    })),
    _.effect(conn => a.eq(conn.get, {
      _id: '1',
      _source: { a: '1', b: 'yo' },
    })('1')),
    _.effect(conn => a.eq(conn.get, {
      _id: '1',
      _source: { a: '1', b: 'yo' },
    })({ _id: '1' })),
    _.effect(conn => conn.update({
      _id: '1',
      _source: { b: 'hi', c: 'hey' },
    })),
    _.effect(conn => a.eq(conn.get, {
      _id: '1',
      _source: { a: '1', b: 'hi', c: 'hey' },
    })('1')),
    _.effect(conn => a.eq(conn.get, {
      _id: '1',
      _source: { a: '1', b: 'hi', c: 'hey' },
    })({ _id: '1' })),
    _.effect(conn => conn.delete('1')),
    _.effect(conn => a.err(
      conn.get,
      exception.ResourceNotFound(`${conn.uri}/1`),
    )('1')),
    _.effect(conn => conn.free()),
    _.effect(conn => conn.client.quit()),
  )({
    connector: 'redis',
    datatype: 'hash',
    env: 'local',
    prefix: 'my_hash',
    endpoint: 'redis://localhost:6379',
  })).timeout(2 * 60 * 1000)
})
