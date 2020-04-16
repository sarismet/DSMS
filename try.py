import struct

Page = open("./Files/5/50", "rb")

Page.seek(7)

ThePage = Page.read(121)
Page.close()

Record_No = ThePage[:1]
Record1 = ThePage[1:5]
Record12 = ThePage[5:9]
print(int(struct.unpack("B", Record_No)[0]))
print(int.from_bytes(Record_No, byteorder="big", signed=False))
print(int.from_bytes(Record12, byteorder="big", signed=False))
# ThePagex = int.from_bytes(ThePage, byteorder='big', signed=False)
# print(ThePage)
