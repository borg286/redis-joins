import redis
import csv

def index(value):
  return float.fromhex(hex(hash(value)))


r = redis.StrictRedis(host='localhost', port=6379, db=0)
#with open('myscript.lua', 'r') as script:
#  script = script.read()
#lua = r.register_script(script)
#print lua(keys=[], args=[])
#exit()

r.flushall()

print (r.set('foo', 2))
print (r.get('foo'))
lua = """
local value = redis.call('GET', KEYS[1])
value = tonumber(value)
return value * ARGV[1]"""
multiply = r.register_script(lua)
print (multiply(keys=['foo'], args=[5]))
with open('pneumon.csv', 'rb') as csvfile:
  reader = csv.reader(csvfile, delimiter=',', quotechar='"')
  first_row = reader.next()[1:]
  for row in reader :
    data = row[1:]
    mapping = dict(zip(first_row, map(int,data)))
    key = "child:" + row[0]
    r.hmset(key, mapping)
    r.zadd("idx:child:age", index(str(mapping["chldage"])),key )

    key = "other:" + row[0]
    r.hmset(key, mapping)
    r.zadd("idx:other:age", index(str(mapping["chldage"])), key)    
