-- KEYS[1] = fire lock key (e.g. "fire-lock:<alertRuleId>:<eventId>")
-- ARGV[1] = node identifier
-- ARGV[2] = TTL in seconds
-- Returns: 1 if this node won the lock (should fire), 0 if already fired

local key = KEYS[1]
local node_id = ARGV[1]
local ttl = tonumber(ARGV[2])

local result = redis.call('SET', key, node_id, 'NX', 'EX', ttl)
if result then
  return 1
else
  return 0
end
