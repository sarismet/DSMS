import struct
import re


def Check_If_Type_Exits(TypeName, SystemCatalog):
    print("Check_If_Type_Exits...")
    if SystemCatalog.NumberOfTypes > 0:
        for i in range(SystemCatalog.NumberOfTypes):
            if SystemCatalog.Types[i] == TypeName:
                return True
    else:
        print("There is no registered types.")

    return False


class FieldsAreNotValid(Exception):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)


class SystemCatalog:
    NumberOfTypes = 0
    Types = []

    def __init__(self):
        try:
            self.SystemCatalogFile = open("SystemCatalog", "rb")
            print("SystemCatalogFile is opened successsively")
            filesize = struct.unpack(
                "i", self.SystemCatalogFile.read(4))[0]
            self.NumberOfTypes = filesize

            print("NUmber Of Type is: ", filesize)
            for i in range(filesize):
                tempTypeName = self.SystemCatalogFile.read(16).decode("utf-8")
                tempTypeName = re.sub(" ", "", tempTypeName)

                print("tempTypeName", tempTypeName)
                tempFileNo = struct.unpack(
                    "b", self.SystemCatalogFile.read(1))[0]
                print("tempFileNo", tempFileNo)
                tempNumberOfFields = struct.unpack(
                    "b", self.SystemCatalogFile.read(1))[0]
                print("tempNumberOfFields", tempNumberOfFields)
                tempfields = []
                for b in range(tempNumberOfFields):
                    print("girdik tek tek toplıcaz")
                    tempx = self.SystemCatalogFile.read(16).decode("utf-8")
                    tempfields.append(tempx)
                print(tempfields)
                T = Type(tempTypeName, tempFileNo,
                         tempNumberOfFields, tempfields)
                self.Types.append(T)

        except FileNotFoundError:
            print("There is no such a file...")
            self.createCatalogFile()
        except Exception as e:
            print(e)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.writeback()

    def createCatalogFile(self):
        print("Creating a new SystemCatalog File...")
        self.SystemCatalogFile = open("SystemCatalog", "wb")
        self.SystemCatalogFile.write(bytes([255]))
        self.SystemCatalogFile.close()

    def readCatalogFile(self):
        print("SystemCatalogFile is being read...")

    def addNewType(self, Type):
        self.NumberOfTypes += 1
        self.Types.append(Type)

    def tamamla(self, TypeName):
        for i in range(len(TypeName), 16):
            TypeName = " "+TypeName
        return TypeName

    def writeback(self):
        print("writing back on System Catalog File...")
        newline = "\n"

        self.SystemCatalogFile = open("SystemCatalog", "wb")
        self.SystemCatalogFile.write(struct.pack("i", self.NumberOfTypes))

        if self.NumberOfTypes > 0:
            print("in writing back we have", self.NumberOfTypes)
            print("Types lenght is :", len(self.Types))
            for i in range(self.NumberOfTypes):
                self.Types[i].TypeName = self.tamamla(self.Types[i].TypeName)
                self.SystemCatalogFile.write(
                    self.Types[i].TypeName.encode("utf-8"))
                self.SystemCatalogFile.write(
                    bytes([self.Types[i].NumberOfFiles]))
                self.SystemCatalogFile.write(
                    bytes([self.Types[i].NumberOfFields]))

                for k in range(self.Types[i].NumberOfFields):
                    self.Types[i].Fields_Names[k] = self.tamamla(
                        self.Types[i].Fields_Names[k])
                    print("returned type fields is :",
                          self.Types[i].Fields_Names[k])
                    self.SystemCatalogFile.write(
                        self.Types[i].Fields_Names[k].encode("utf-8"))
        self.SystemCatalogFile.close()


class Type:
    TypeName = ""
    NumberOfFields = 0
    NumberOfFiles = 0
    Fields_Names = []

    def __init__(self, TypeName, NumberOfFiles, NumberOfFields, Fields_Names):
        self.control()
        self.TypeName = TypeName
        self.NumberOfFields = NumberOfFields
        self.NumberOfFiles = NumberOfFiles
        self.Fields_Names = Fields_Names

    def control(self):
        if(self.NumberOfFields > 64) or len(self.Fields_Names) != self.NumberOfFields:
            raise FieldsAreNotValid(
                "Number of fields are not in desired interval")


class DLL:

    def __init__(self, SystemCatalog):
        self.SystemCatalog = SystemCatalog

    def Create_Type(self, TypeName, N, Fields_Names):
        # Check_If_Type_Exits(TypeName, self.SystemCatalog)
        print("Creating Type...")
        newType = Type(TypeName, 0, N, Fields_Names)
        self.SystemCatalog.addNewType(newType)


with SystemCatalog() as f:
    r2 = DLL(f)

    r2.Create_Type("Humans", 3, ["age", "len", "spe"])  # 16
    r2.Create_Type("Cats", 4, ["age", "len", "spe", "smell"])  # 18

with SystemCatalog() as f:
    r2 = DLL(f)

    r2.Create_Type("Aliens", 3, ["ageA", "lenA", "speA"])  # 16
    r2.Create_Type("CatsA", 4, ["ageA", "lenA", "speA", "smellA"])  # 18

with SystemCatalog() as f:

    f.readCatalogFile()

"""
with SystemCatalog() as f:

    f.readCatalogFile()





def write(TypeName, No, Lists):
    f.write(TypeName.encode("utf-8"))
    f.write(bytes([No]))
    for i in range(len(Lists)):
        f.write(Lists[i].encode("utf-8"))


f = open("file", "wb")
f.write(bytes([12]))

write("Humans", 3, ["yas", "boy", "hız"])
f.close()
"""
