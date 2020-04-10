import struct
import re
import os


class FieldsAreNotValid(Exception):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)


def Check_If_Type_Exits(TypeName, SystemCatalog):
    print("Checking If Type Exits...")
    if len(SystemCatalog.Types) > 0:
        for Type in SystemCatalog.Types:
            if Type.TypeName == TypeName:
                print("The Type is found!!! ")
                return Type
    else:
        print("There is no registered types.")

    return None


class SystemCatalog:

    def __init__(self):
        self.NumberOfTypes = 0
        self.Types = []
        self.indexFiles = {}
        self.ismet = ""
        self.SystemCatalogFile = None
        if os.path.exists("SystemCatalog"):
            print("SystemCatalogFile is opened successsively")
            self.readCatalogFile()
            self.readindexFiles()
        else:

            print("There is no such a file...")
            self.createCatalogFile()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.writeback()
        self.writebackindexFiles()

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
                print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
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
        print("All indexFiles are being read...")
        for Type in self.Types:
            filename = "./indexFiles/"+str(Type.TypeName)+"index"
            TindexFile = open(filename, "rb")
            NumberOfRecords = struct.unpack(
                "i", TindexFile.read(4))[0]
            NumberOfRecordsPerFile = struct.unpack(
                "i", TindexFile.read(4))[0]
            print("In related indexFile we found")
            print("The Number Of Records ", NumberOfRecords)
            print("The Number Of Records Per File", NumberOfRecordsPerFile)
            Temp_Records_Array = []
            for i in range(NumberOfRecords):
                FileID = struct.unpack(
                    "b", self.SystemCatalogFile.read(1))[0]
                PageID = struct.unpack(
                    "b", self.SystemCatalogFile.read(1))[0]
                RecordID = struct.unpack(
                    "b", self.SystemCatalogFile.read(1))[0]
                PrimaryKey = struct.unpack(
                    "i", self.SystemCatalogFile.read(4))[0]
                Temp_Records_Array.append(
                    Record(FileID, PageID, RecordID, PrimaryKey))

            TI = indexFile(NumberOfRecords, NumberOfRecordsPerFile,
                           Temp_Records_Array)
            self.indexFiles.update({Type: TI})
            TindexFile.close()

    def writebackindexFiles(self):
        print("All indexFiles are being written back...")
        for key in self.indexFiles:
            filename = "./indexFiles/"+str(key)+"index"
            TindexFile = open(filename, "wb")
            TindexFile.write(struct.pack(
                "i", self.indexFiles[key].Number_OF_Records))
            TindexFile.write(struct.pack(
                "i", self.indexFiles[key].Max_Number_OF_Records_Per_File))
            for record in self.indexFiles[key].Records:
                TindexFile.write(
                    bytes([record.FileID]))
                TindexFile.write(
                    bytes([record.PageID]))
                TindexFile.write(
                    bytes([record.RecordID]))
                TindexFile.write(struct.pack("i", record.PrimaryKey))

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
            for Type in self.Types:
                TypeName = self.complate(Type.TypeName)
                self.SystemCatalogFile.write(
                    TypeName.encode("utf-8"))
                self.SystemCatalogFile.write(
                    bytes([Type.NumberOfFiles]))
                self.SystemCatalogFile.write(
                    bytes([Type.NumberOfFields]))

                for i in range(Type.NumberOfFields):
                    FN = self.complate(Type.Fields_Names[i])
                    self.SystemCatalogFile.write(FN.encode("utf-8"))

            self.SystemCatalogFile.close()


class Type:

    def __init__(self, TypeName, NumberOfFiles, NumberOfFields, Fields_Names):

        self.TypeName = TypeName
        self.NumberOfFiles = NumberOfFiles
        self.NumberOfFields = NumberOfFields
        self.Fields_Names = Fields_Names


class Record:
    def __init__(self, FileID, PageID, RecordID, PrimaryKey):
        self.FileID = FileID
        self.PageID = PageID
        self.RecordID = RecordID
        self.PrimaryKey = PrimaryKey


class indexFile:

    def __init__(self, Number_OF_Records, Max_Number_OF_Records_Per_File, Records):
        self.Number_OF_Records = Number_OF_Records
        self.Max_Number_OF_Records_Per_File = Max_Number_OF_Records_Per_File
        self.Records = Records


class DLL:
    def __init__(self, SystemCatalog):
        self.SystemCatalog = SystemCatalog

    def Create_Type(self, TypeName, NumberOfFields, Fields_Names):
        if Check_If_Type_Exits(TypeName, self.SystemCatalog) == None:
            print("Type is available to create\nCreating Type...")
            filename = "./indexFiles/"+str(TypeName)+"index"
            indexFile = open(filename, "wb")
            maxNoofRecords = int(2048/(4*NumberOfFields))
            indexFile.write(struct.pack("i", 0))
            indexFile.write(struct.pack("i", maxNoofRecords))
            indexFile.close()
            newType = Type(TypeName, 0, NumberOfFields, Fields_Names)
            self.SystemCatalog.addNewType(newType)

    def List_All_Types(self):
        for i in range(self.SystemCatalog.NumberOfTypes):
            x = self.SystemCatalog.Types[i].TypeName

    def List_All_Types(self):
        print("Listing all types...")
        for Type in self.SystemCatalog.Types:
            print(Type.TypeName)


class DML:

    def __init__(self, SystemCatalog):
        self.SystemCatalog = SystemCatalog

    def Create_Record(self, TypeName, Fields_Values):
        print("Creating Record")
        filename = "./indexFiles/"+str(TypeName)+"index"
        indexFile = open(filename, "rb")
        indexFile.seek(4)
        MaxNumberOfRecordsPerFile = struct.unpack(
            "i", indexFile.read(4))[0]
        print(MaxNumberOfRecordsPerFile, " is read from indexFile of ", TypeName)
        MaxNumberOfRecordsPerPage = int(MaxNumberOfRecordsPerFile/256)


if not os.path.exists("indexFiles"):
    os.mkdir("indexFiles")


with SystemCatalog() as f:
    d1 = DLL(f)
    d2 = DML(f)
    d1.Create_Type("Humans", 3, ["age", "len", "spe"])  # 16
    d1.Create_Type("Cats", 4, ["age", "len", "spe", "smell"])  # 18
    d1.Create_Type("Humans", 3, ["age", "len", "spe"])  # 16

    d2.Create_Record("Humans", [23, 172, 45])

print()
print()
with SystemCatalog() as iso:

    print("Burası cok onemliiii ", len(iso.Types))

    d1 = DLL(iso)
    d1.List_All_Types()
