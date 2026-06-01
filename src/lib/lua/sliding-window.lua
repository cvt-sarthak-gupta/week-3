-- KEYS[1] = rate limit key (e.g. "rl:apikey:<key>")
-- ARGV[1] = window size in milliseconds
-- ARGV[2] = max requests allowed
-- ARGV[3] = current timestamp in milliseconds
-- ARGV[4] = unique request identifier (nanoid, used as prefix for batch entries)
-- ARGV[5] = number of slots to consume (default 1; pass batch size for bulk ingestion)
-- Returns: {allowed (0/1), remaining, reset_at_ms}

local key = KEYS[1]
local window_ms = tonumber(ARGV[1])
local max_requests = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local req_id = ARGV[4]
local count = tonumber(ARGV[5]) or 1
local window_start = now - window_ms

-- Remove entries outside the current window
redis.call('ZREMRANGEBYSCORE', key, 0, window_start)

-- Count current entries
local current = redis.call('ZCARD', key)

if current + count <= max_requests then
  -- Allow: add `count` entries, each with a unique member to avoid ZADD dedup
  for i = 1, count do
    redis.call('ZADD', key, now, req_id .. ':' .. i)
  end
  redis.call('PEXPIRE', key, window_ms)
  return {1, max_requests - current - count, now + window_ms}
else
  -- Denied: get the oldest entry to calculate reset time
  local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
  local reset_at = now + window_ms
  if oldest and #oldest >= 2 then
    reset_at = tonumber(oldest[2]) + window_ms
  end
  return {0, 0, reset_at}
end
