package redisx

// ProducerLuaScript 定义包含数据落存及其同时绑定派发相关流处理等业务强关联逻辑的安全同步执行化执行事务指令体集合块
const ProducerLuaScript = `
-- KEYS[1] = Dedupe Key
-- KEYS[2] = Stream Name
-- ARGV[1] = Event ID
-- ARGV[2] = TTL Seconds
-- ARGV[3] = Session ID
-- ARGV[4] = Payload
-- ARGV[5] = Produced At

local inserted = redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', ARGV[2])
if not inserted then
	return ''
end

local msg_id = redis.call(
	'XADD',
	KEYS[2],
	'*',
	'event_id', ARGV[1],
	'session_id', ARGV[3],
	'payload', ARGV[4],
	'produced_at', ARGV[5]
)
return msg_id
`
