local zset = redis.call('zrangebyscore', KEYS[2], ARGV[1], ARGV[2], 'withscores')
local i = 1
local scores = {}
local tosend = {}
while(i <= #zset) do
  if scores[zset[i+1]] == nil then
    scores[zset[i+1]] = 1
    tosend[1+#tosend] = zset[i+1]
  end
  if #tosend >= 1000 then
    redis.breakpoint()
    redis.call('sadd', KEYS[1], unpack(tosend))
    tosend = {}
  end
  i = i + 2
end
if (#tosend > 0) then
  redis.call('sadd', KEYS[1], unpack(tosend))
end
return (#scores)/2
