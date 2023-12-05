id = ID(234525)
print(id.hex())
print(id.denary())
print(id.bin())
print(type(id.bin()))
print(id.__str__())

try:
    id = ID(2**160)
    print(id.hex())
except ValueError as e:
    print(e)

try:
    id = ID(2**160 - 2)
    print(id.hex())
    print(id.bin())
except ValueError as e:
    print(e)

node_1 = Node(id=ID(5), port=1234, IP="127.0.0.1")
node_2 = Node(id=ID(7), IP="192.168.1.1", port=2346)
print(node_1.distance(node_2.id.denary()))

node_1 = Node(id=ID(5), port=1234, IP="127.0.0.1")
node_2 = Node(id=ID(8), IP="192.168.1.1", port=2346)
print(node_1.distance(node_2.id.denary()))

node_1 = Node(id=ID(50), port=1234, IP="127.0.0.1")
node_2 = Node(id=ID(700), IP="192.168.1.1", port=2346)
print(node_1.distance(node_2.id.denary()))