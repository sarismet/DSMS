import struct
import re
import os


class SystemCatalog:

    def __init__(self):
        self.NumberOfTypes = 0
        self.Types = []
        self.indexFiles = []
        self.ismet = ""
        self.SystemCatalogFile = None
        if os.path.exists("SystemCatalog"):
            print("SystemCatalogFile is opened successsively")
            self.readCatalogFile()
        else:

            print("There is no such a file...")
            self.createCatalogFile()

    def createCatalogFile(self):
        print("Creating a new SystemCatalog File...")
        open("SystemCatalog", "a").close()

    def readCatalogFile(self):
        print("SystemCatalogFile is being read...")
        try:
            self.SystemCatalogFile = open("SystemCatalog", "rb")
            self.NumberOfTypes = struct.unpack(
                "i", self.SystemCatalogFile.read(4))[0]
            print(self.NumberOfTypes, " is read from SystemCatalog ")

            for i in range(self.NumberOfTypes):
                Type_Name = self.SystemCatalogFile.read(
                    16).decode("utf-8")
                Type_Name = re.sub(" ", "", Type_Name)

                File_No = struct.unpack(
                    "b", self.SystemCatalogFile.read(1))[0]
                Fields_No = struct.unpack(
                    "b", self.SystemCatalogFile.read(1))[0]
                Fields = []
                for b in range(Fields_No):

                    tempx = self.SystemCatalogFile.read(16).decode("utf-8")
                    Fields.append(tempx)
                T = Type(Type_Name, File_No,
                         Fields_No, Fields)
                self.Types.append(T)

            self.SystemCatalogFile.close()
            print("Reading is Compleated")
        except Exception as e:
            print(e)
            self.SystemCatalogFile.close()
            os._exit(0)

    def readindexFiles(self):
        print()

    def addNewType(self, Type):
        print("Increasing Number Of Files...")
        self.NumberOfTypes += 1
        print("Adding the new type to Types...")
        self.Types.append(Type)

    def complate(self, TypeName):
        for i in range(len(TypeName), 16):
            TypeName = " "+TypeName
        return TypeName

    def writeback(self):
        if self.NumberOfTypes > 0:
            print("writing back on System Catalog File...")
            self.SystemCatalogFile = open("SystemCatalog", "wb")
            self.SystemCatalogFile.write(struct.pack("i", self.NumberOfTypes))
            for T in self.Types:
                TN = self.complate(T.TypeName)
                self.SystemCatalogFile.write(
                    TN.encode("utf-8"))
                self.SystemCatalogFile.write(
                    bytes([T.NumberOfFiles]))
                self.SystemCatalogFile.write(
                    bytes([T.NumberOfFields]))

                for i in range(T.NumberOfFields):
                    FN = self.complate(T.Fields_Names[i])
                    self.SystemCatalogFile.write(FN.encode("utf-8"))

            self.SystemCatalogFile.close()

    def __del__(self):
        print('Destructor called')
        self.writeback()


class Type:

    def __init__(self, TypeName, NumberOfFiles, NumberOfFields, Fields_Names):

        self.TypeName = TypeName
        self.NumberOfFiles = NumberOfFiles
        self.NumberOfFields = NumberOfFields
        self.Fields_Names = Fields_Names


class indexFile:

    def __init__(self, Number_OF_Records, Max_Number_OF_Records_Per_File, Records):
        self.Number_OF_Records = Number_OF_Records
        self.Max_Number_OF_Records_Per_File = Max_Number_OF_Records_Per_File
        self.Records = Records


def Create_Type(TypeName, NumberOfFields, Fields_Names, SystemCatalog):

    print("Type is available to create\nCreating Type...")
    filename = "./indexFiles/"+str(TypeName)+"index"
    indexFile = open(filename, "wb")
    maxNoofRecords = int(2048/(4*NumberOfFields))
    indexFile.write(struct.pack("i", 0))
    indexFile.write(struct.pack("i", maxNoofRecords))
    indexFile.close()
    newType = Type(TypeName, 0, NumberOfFields, Fields_Names)
    SystemCatalog.addNewType(newType)


def List_All_Types(SystemCatalog):
    for i in range(SystemCatalog.NumberOfTypes):
        x = SystemCatalog.Types[i].TypeName


if not os.path.exists("indexFiles"):
    os.mkdir("indexFiles")


f = SystemCatalog()

Create_Type("Humans", 3, ["age", "len", "spe"], f)  # 16
Create_Type("Cats", 4, ["age", "len", "spe", "smell"], f)  # 18
del f

print()
print()
iso = SystemCatalog()

print("Burası cok onemliiii ", len(iso.Types))

Create_Type("Humans", 3, ["age", "len", "spe"], iso)  # 16
Create_Type("Cats", 4, ["age", "len", "spe", "smell"], iso)  # 18
del iso
