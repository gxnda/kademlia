import hashlib
h = hashlib.sha3_256()
"""
This appears to be the latest version, however, I am not sure what size messages I will be
sending, if they are small messages, apparently KangarooTwelve is better for these
size messages (see
https://en.wikipedia.org/wiki/SHA-3 for details). If this is feasible I will see what is
defined by a "small" message etc.
"""
h.update(b"HelloWorld.txt")
print(h.hexdigest())