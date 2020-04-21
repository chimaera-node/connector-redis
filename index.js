const _ = require('rubico')
const exception = require('@chimaera/exception')
const uri = require('@chimaera/uri')
const redis = require('redis')

const getKeyPrefix = config => `${config.env}:${config.prefix}`

// object => object => () => undefined
const init = config => client => _.flow(
  () => getKeyPrefix(config),
  _.diverge([_.id, _.id]),
  _.spread(client.sadd),
  _.noop,
)

// object => object => () => undefined
const free = config => client => _.flow(
  () => getKeyPrefix(config),
  client.smembers,
  _.switch(
    _.eq(_.size, 0),
    _.id,
    _.spread(client.del),
  ),
  _.noop,
)

// object => object => () => undefined
const ready = config => client => _.flow(
  () => getKeyPrefix(config),
  client.scard,
  _.switch(
    _.gte(_.id, 1),
    _.noop,
    _.flow(
      () => uri.encodePublic(config),
      exception.ConnectionNotReady,
      _.throw,
    ),
  ),
)

const saveKeyToIndex = config => client => _.flow(
  _.diverge([getKeyPrefix(config), _.get('_id')]),
  _.spread(client.sadd),
)

const absKey = config => _.flow(
  _.diverge([
    getKeyPrefix(config),
    _.switch(
      _.isString, _.id,
      _.isObject, _.get('_id'),
      _.flow(x => new TypeError(x), _.throw),
    ),
  ]),
  _.join(':'),
)

// object => object => object => undefined
const put = config => {
  if (config.datatype === 'string') return client => _.flow(
    _.diverge([
      saveKeyToIndex(config)(client),
      _.flow(
        _.diverge([absKey(config), _.get('_source')]),
        _.spread(client.set),
      ),
    ]),
    _.noop,
  )
  if (config.datatype === 'set') return client => _.flow(
    _.diverge([
      saveKeyToIndex(config)(client),
      _.flow(
        _.diverge([absKey(config), _.get('_source')]),
        _.flatten,
        _.effect(_.flow(_.get(0), client.del)),
        _.spread(client.sadd),
      ),
    ]),
    _.noop,
  )
  if (config.datatype === 'sorted_set') return client => _.flow(
    _.diverge([
      saveKeyToIndex(config)(client),
      _.flow(
        _.diverge([absKey(config), _.get('_source')]),
        _.flatten,
        _.effect(_.flow(_.get(0), client.del)),
        _.spread(client.zadd),
      ),
    ]),
    _.noop,
  )
  if (config.datatype === 'hash') return client => _.flow(
    _.diverge([
      saveKeyToIndex(config)(client),
      _.flow(
        _.diverge([
          absKey(config),
          _.flow(_.get('_source'), Object.entries, _.flatten),
        ]),
        _.flatten,
        _.effect(_.flow(_.get(0), client.del)),
        _.spread(client.hset),
      ),
    ]),
    _.noop,
  )
  throw new RangeError(`invalid datatype ${config.datatype}`)
}

const toID = x => {
  if (_.isString(x)) return x
  if (_.isObject(x)) return x._id
  throw new TypeError(`cannot get id: ${_.shorthand(x)}`)
}

const exceptNotFound = config => _.flow(
  _.diverge([uri.encodePublic(config), _.get('_id')]),
  _.join('/'),
  exception.ResourceNotFound,
  e => {
    Error.captureStackTrace(e, exceptNotFound)
    return e
  },
  _.throw,
)

// object => object => string|object => object
const get = config => {
  if (config.datatype === 'string') return client => _.flow(
    toID,
    _.diverge({
      _id: _.id,
      _source: _.flow(absKey(config), client.get),
    }),
    _.switch(
      _.flow(_.get('_source'), _.exists),
      _.id,
      exceptNotFound(config),
    ),
  )
  if (config.datatype === 'set') return client => _.flow(
    toID,
    _.diverge({
      _id: _.id,
      _source: _.flow(absKey(config), client.smembers),
    }),
    _.switch(
      _.flow(_.get('_source'), _.gt(_.size, 0)),
      _.id,
      exceptNotFound(config),
    ),
  )
  if (config.datatype === 'sorted_set') return client => _.flow(
    _.diverge({
      _id: toID,
      _source: _.switch(_.has('with_scores'), _.flow(
        toID,
        _.diverge([absKey(config), 0, -1, 'WITHSCORES']),
        _.spread(client.zrange),
      ), _.flow(
        toID,
        _.diverge([absKey(config), 0, -1]),
        _.spread(client.zrange),
      )),
    }),
    _.switch(
      _.flow(_.get('_source'), _.gt(_.size, 0)),
      _.id,
      exceptNotFound(config),
    ),
  )
  if (config.datatype === 'hash') return client => _.flow(
    toID,
    _.diverge({
      _id: _.id,
      _source: _.flow(absKey(config), client.hgetall),
    }),
    _.switch(
      _.flow(_.get('_source'), _.exists),
      _.id,
      exceptNotFound(config),
    ),
  )
  throw new RangeError(`invalid datatype ${config.datatype}`)
}

// object => object => object => undefined
const update = config => {
  if (config.datatype === 'string') return put(config)
  if (config.datatype === 'set') return client => _.flow(
    _.diverge([
      saveKeyToIndex(config)(client),
      _.flow(
        _.diverge([absKey(config), _.get('_source')]),
        _.flatten,
        _.spread(client.sadd),
      ),
    ]),
    _.noop,
  )
  if (config.datatype === 'sorted_set') return client => _.flow(
    _.diverge([
      saveKeyToIndex(config)(client),
      _.flow(
        _.diverge([absKey(config), _.get('_source')]),
        _.flatten,
        _.spread(client.zadd),
      ),
    ]),
    _.noop,
  )
  if (config.datatype === 'hash') return client => _.flow(
    _.diverge([
      saveKeyToIndex(config)(client),
      _.flow(
        _.diverge([
          absKey(config),
          _.flow(_.get('_source'), Object.entries, _.flatten),
        ]),
        _.flatten,
        _.spread(client.hset),
      ),
    ]),
    _.noop,
  )
  throw new RangeError(`invalid datatype ${config.datatype}`)
}

// object => object => string => undefined
const del = config => client => _.flow(
  _.diverge([getKeyPrefix(config), _.id]),
  _.join(':'),
  _.diverge([
    _.flow(
      _.diverge([getKeyPrefix(config), _.id]),
      _.spread(client.srem),
    ),
    client.del,
  ]),
  _.noop,
)

/*
 * uri => connection {
 *   uri: string,
 *   prefix: string,
 *   client: object,
 *   ready: () => boolean,
 *   put: object => boolean,
 *   get: string => object,
 *   update: object => object,
 *   delete: string => boolean,
 *   init: () => boolean,
 *   free: () => boolean,
 * }
 */
const connectorRedis = _.flow(
  // datatype: string, set, sorted_set, hash
  _.diverge([
    _.id,
    _.flow(
      _.get('endpoint'),
      redis.createClient,
      _.promisifyAll,
    ),
  ]),
  _.diverge({
    uri: _.flow(_.get(0), uri.encodePublic),
    prefix: _.get([0, 'prefix']),
    client: _.get(1),
    ready: _.apply(ready),
    put: _.apply(put),
    get: _.apply(get),
    update: _.apply(update),
    delete: _.apply(del),
    init: _.apply(init),
    free: _.apply(free),
  })
)

module.exports = connectorRedis
