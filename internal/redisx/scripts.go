package redisx

// ProducerLuaScript 定义包含数据落存及其同时绑定派发相关流处理等业务强关联逻辑的安全同步执行化执行事务指令体集合块
const ProducerLuaScript = `
-- KEYS[1] = Session Cache Key
-- KEYS[2] = Stream Name
-- ARGV[1] = Session Payload
-- ARGV[2] = TTL
-- ARGV[3] = Stream Payload

redis.call('SETEX', KEYS[1], ARGV[2], ARGV[1])
local msg_id = redis.call('XADD', KEYS[2], '*', 'payload', ARGV[3])
return msg_id
`
