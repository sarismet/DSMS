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


def insert_Record_To_indexFile(PrimaryKey, TheindexFile):
    index = 0
    RID, FID, PID = 1, 1, 1
    print(TheindexFile.Records)
    for RecordN in TheindexFile.Records:

        if RecordN.PrimaryKey > PrimaryKey:
            RID = RecordN.RecordID
            FID = RecordN.FileID
            PID = RecordN.PageID
            break
        index += 1
    if RID == int(TheindexFile.Max_Number_OF_Records_Per_File/256):
        RID = 1
        PID += 1
    if PID == 256:
        PID = 1
        FID += 1
    newRecord = Record(FID, PID, RID, PrimaryKey)
    TheindexFile.Records.insert(index, newRecord)
    TheindexFile.Number_OF_Records += 1
    return(index, newRecord)


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


class classindexFile:

    def __init__(self, Number_OF_Records, Max_Number_OF_Records_Per_File, Records):
        self.Number_OF_Records = Number_OF_Records
        self.Max_Number_OF_Records_Per_File = Max_Number_OF_Records_Per_File
        self.Records = Records


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
            try:
                filepath = "./indexFiles/"+str(Type.TypeName)+"index"
                print(filepath)
                TindexFile = open(filepath, "rb")
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
                        "b", TindexFile.read(1))[0]
                    PageID = struct.unpack(
                        "b", TindexFile.read(1))[0]
                    RecordID = struct.unpack(
                        "b", TindexFile.read(1))[0]
                    PrimaryKey = struct.unpack(
                        "i", TindexFile.read(4))[0]
                    Temp_Records_Array.append(
                        Record(int(FileID), int(PageID), int(RecordID), int(PrimaryKey)))

                TI = classindexFile(NumberOfRecords, NumberOfRecordsPerFile,
                                    Temp_Records_Array)
                print(
                    "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                a = TI.Records
                self.indexFiles.update({Type.TypeName: TI})
                TindexFile.close()
            except Exception as e:
                print(e)

    def writebackindexFiles(self):
        print("All indexFiles are being written back...")
        for x in self.indexFiles:
            print("KEYS,", self.indexFiles[x])
        for key in self.indexFiles:
            filename = "./indexFiles/"+str(key)+"index"
            TindexFile = open(filename, "wb")
            TindexFile.write(struct.pack(
                "i", self.indexFiles[key].Number_OF_Records))
            TindexFile.write(struct.pack(
                "i", self.indexFiles[key].Max_Number_OF_Records_Per_File))
            if len(self.indexFiles[key].Records) > 0:
                for record in self.indexFiles[key].Records:
                    print("AAAAAAAAAAAAAAAAAAAAAAA", record.FileID)
                    TindexFile.write(
                        bytes([int(record.FileID)]))
                    TindexFile.write(
                        bytes([int(record.PageID)]))
                    TindexFile.write(
                        bytes([int(record.RecordID)]))
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

            self.SystemCatalog.indexFiles.update(
                {TypeName: classindexFile(0, maxNoofRecords, [])})
        else:
            print("Type is not going to be written!!!!!!!")

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
        for x in self.SystemCatalog.indexFiles:
            print("keys", self.SystemCatalog.indexFiles[x])
        TuppleReturned = insert_Record_To_indexFile(
            Fields_Values[0], self.SystemCatalog.indexFiles[TypeName])

        print("we placed the new record at",
              TuppleReturned[0], "th index")
        print(" FileID is:", TuppleReturned[1].FileID, " PageID is:", TuppleReturned[1].PageID,
              " RecordID is:", TuppleReturned[1].RecordID, " PK is:", TuppleReturned[1].PrimaryKey)


if not os.path.exists("indexFiles"):
    os.mkdir("indexFiles")


with SystemCatalog() as f:
    d1 = DLL(f)
    d2 = DML(f)
    d1.Create_Type("Humans", 3, ["age", "len", "spe"])  # 16
    # d1.Create_Type("Cats", 4, ["age", "len", "spe", "smell"])  # 18
    # d1.Create_Type("Humans", 3, ["age", "len", "spe"])  # 16

    d2.Create_Record("Humans", [23, 172, 45])
