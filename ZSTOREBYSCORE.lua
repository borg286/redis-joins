local zset = redis.call('zrangebyscore', KEYS[2], ARGV[1], ARGV[2], 'withscores')
local i = 1
local temp = {}
while(i <= #zset) do
  temp[1+#temp] = zset[i+1]
  temp[1+#temp] = zset[i]
  if #zset >= 1000 then
    redis.call('zadd', KEYS[1], unpack(temp))
    table = {}
  end
  i = i + 2
end
if (#temp > 0) then
  redis.call('zadd', KEYS[1], unpack(temp))
end
return #zset/2
