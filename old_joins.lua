-- Take 2 sorted sets and join by their score storing intersection in another sorted set
-- Returns min and max score of intersection
local zset = redis.call("zrangebyscore",KEYS[2], ARGV[1], ARGV[2], "WITHSCORES")  -- the first sorted set.
local previous = {}  -- The list of elements with the same score, namely that of the last observed score
local set1 = {}  -- Map<score, list<element>> for the first sorted set

for i, v in ipairs(zset)
do
  if (i % 2 == 1) then  -- 1 indexed list where odds are the elements of the zset
    previous[1+#previous] = v
  else                  --    and the evens are the scores
    if (set1[v] == nil) then
      set1[v] = {}
    end
    previous = set1[v]

    -- Handle corner case of first element getting forgotten
    if (i == 2) then
      previous[1+#previous] = zset[1]
    end
  end
end


zset = redis.call("zrangebyscore",KEYS[3], ARGV[1], ARGV[2], "WITHSCORES")
local element = 0
local ret = {}
local minObservedScore = nil
local maxObservedScore = nil

-- Buffered save to bypass unpacking too many elements
local function save(score, element)
  ret[1+#ret] = score
  ret[1+#ret] = element
  if (#ret > 20) then
    redis.call("zadd", KEYS[1], unpack(ret))
    ret = {}
  end
end

-- Walk through the other sorted set and save elements with previously observed scores
for i, score in ipairs(zset)
do
  if (i % 2 == 1) then
    element = score
  else
    if (set1[score]) then  -- Check if this score was in the first sorted set
      save(score, element)

      if (minObservedScore == nil or score < minObservedScore) then
        minObservedScore = score
      end
      if (maxObservedScore == nil or score > maxObservedScore) then
        maxObservedScore = score
      end

      -- Consume all elements in first sorted set for this matching score
      if #set1[score] ~= 0 then
        for j, ov in ipairs(set1[score])
        do
          save(score, ov)
        end
        set1[score] = {}   -- Keep around score for matching purposes
      end
    end
  end
end

-- Flush out remaining elements
redis.call("zadd", KEYS[1], unpack(ret))

return {minObservedScore, maxObservedScore}
