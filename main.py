import struct
import re
import os


def Check_If_Type_Exits(TypeName, SystemCatalog):
    print("Checking If Type Exits...")
    The_Type = None
    if SystemCatalog.NumberOfTypes > 0:
        for i in range(SystemCatalog.NumberOfTypes):
            if SystemCatalog.Types[i].TypeName == TypeName:
                print("The Type is found!!! ")
                return SystemCatalog.Types[i]
    else:
        print("There is no registered types.")

    return The_Type


def record_Control(TypeName, NumberOfFields, PrimaryKey, SystemCatalog):
    if Check_If_Type_Exits(TypeName, SystemCatalog) == None:
        print("The File you want to add a record in is not found!!!")
        os._exit(0)
    else:
        if NumberOfFields > 64 or NumberOfFields < 3:
            print("The number of Fields are not in desired interval")
        else:
            path = "./indexFiles/"+str(TypeName)+"index"
            indexFile = open(path, "rb")
            CurrentNumberOfRecords = struct.unpack(
                "i", indexFile.read(4))[0]
            MaxNumberOfRecordsPerFile = struct.unpack(
                "i", indexFile.read(4))[0]
            PeekNumberOfRecords = 256*MaxNumberOfRecordsPerFile
            if CurrentNumberOfRecords == PeekNumberOfRecords:
                print("The system cannot accept one more record!!!")
            else:
                print("checking if the given Primary Key is already being used...")
                TheindexFile = SystemCatalog.indexFiles[TypeName]
                for i in TheindexFile.Records:
                    if i.PrimaryKey == PrimaryKey:
                        print("The primary key is alread being used!!!")
                        os._exit(0)


class FieldsAreNotValid(Exception):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)


class SystemCatalog:
    NumberOfTypes = 0
    Types = []
    indexFiles = []

    def __init__(self):
        try:
            self.SystemCatalogFile = open("SystemCatalog", "rb")
            print("SystemCatalogFile is opened successsively")
            self.readCatalogFile()
            # self.readindexFiles()

        except FileNotFoundError:
            print("There is no such a file...")
            self.createCatalogFile()

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
        try:
            self.NumberOfTypes = struct.unpack(
                "i", self.SystemCatalogFile.read(4))[0]

            for i in range(self.NumberOfTypes):
                tempTypeName = self.SystemCatalogFile.read(16).decode("utf-8")
                tempTypeName = re.sub(" ", "", tempTypeName)

                tempFileNo = struct.unpack(
                    "b", self.SystemCatalogFile.read(1))[0]

                tempNumberOfFields = struct.unpack(
                    "b", self.SystemCatalogFile.read(1))[0]

                tempfields = []
                for b in range(tempNumberOfFields):

                    tempx = self.SystemCatalogFile.read(16).decode("utf-8")
                    tempfields.append(tempx)

                T = Type(tempTypeName, tempFileNo,
                         tempNumberOfFields, tempfields)
                self.Types.append(T)
            print("Compleated")

        except Exception as e:
            print(e)
            os._exit(0)

    def readindexFiles(self):
        print("The All indexFiles are being read")
        if self.NumberOfTypes > 0:
            for i in self.Types:
                indexFile = open(str(i.TypeName)+"index")

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
            #print("in writing back we have", self.NumberOfTypes)
            #print("Types lenght is :", len(self.Types))
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
        if(self.NumberOfFields > 64) or(len(self.TypeName) > 16)or(len(self.Fields_Names) != self.NumberOfFields):
            raise FieldsAreNotValid(
                "Number of fields are not in desired interval")


class indexFile:
    Number_OF_Records = 0
    Max_Number_OF_Records_Per_File = 0
    Records = []

    def __init__(self, Number_OF_Records, Max_Number_OF_Records_Per_File, Records):
        self.Number_OF_Records = Number_OF_Records
        self.Max_Number_OF_Records_Per_File = Max_Number_OF_Records_Per_File
        self.Records = Records


class DLL:

    def __init__(self, SystemCatalog):
        self.SystemCatalog = SystemCatalog

    def Create_Type(self, TypeName, N, Fields_Names):
        if Check_If_Type_Exits(TypeName, self.SystemCatalog) == None:
            print("Type is available to create\nCreating Type...")
            filename = "./indexFiles/"+str(TypeName)+"index"
            indexFile = open(filename, "wb")
            maxNoofRecords = int(2048/(4*N))
            indexFile.write(struct.pack("i", 0))
            indexFile.write(struct.pack("i", maxNoofRecords))
            indexFile.close()
            newType = Type(TypeName, 0, N, Fields_Names)
            self.SystemCatalog.addNewType(newType)

    def Delete_Type(self, TypeName):
        if Check_If_Type_Exits(TypeName, self.SystemCatalog) != None:
            print("Type is found.\nDeleting Type...")
            filename = "./indexFiles/"+str(TypeName)+"index"
            os.remove(filename)

            for i in self.SystemCatalog.Types:
                if i.TypeName == TypeName:
                    if i.NumberOfFiles > 0:
                        print("deleting the file")

    def List_All_Types(self):
        for i in range(self.SystemCatalog.NumberOfTypes):
            x = self.SystemCatalog.Types[i].TypeName
            print(re.sub(" ", "", x))


class DML:

    def __init__(self, SystemCatalog):
        self.SystemCatalog = SystemCatalog

    def Create_Record(self, TypeName, Fields_Values):
        print("Creating Record")


if not os.path.exists("indexFiles"):
    os.mkdir("indexFiles")
with SystemCatalog() as f:
    r2 = DLL(f)

    r2.Create_Type("Humans", 3, ["age", "len", "spe"])  # 16
    r2.Create_Type("Cats", 4, ["age", "len", "spe", "smell"])  # 18

with SystemCatalog() as fx:
    print(len(fx.Types))
