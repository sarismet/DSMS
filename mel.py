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


def LinkFilesbetween(TypeName, Previous, Next):
    print("Linking between Files")
    MutualPath = "Files/" + str(TypeName) + "/" + str(TypeName)
    FirstFile = open(MutualPath + str(Previous), "wb")
    SecondFile = open(MutualPath + str(Next), "wb")
    FirstFile.seek(6)
    SecondFile.seek(7)
    FirstFile.write(bytes([int(Next)]))
    SecondFile.write(bytes([int(Previous)]))


def insert_Record_To_indexFile(PrimaryKey, TheindexFile):
    TheindexFile.Number_OF_Records += 1
    if len(TheindexFile.Records) == 0:
        newRecord = Record(0, 0, 0, PrimaryKey)
        TheindexFile.Records.append(newRecord)
        return -1
    index = 0

    RID, FID, PID = 1, 1, 1
    print(TheindexFile.Records)
    for RecordN in TheindexFile.Records:

        if RecordN.PrimaryKey > PrimaryKey:
            break
        index += 1

    if index == 0:
        FID = TheindexFile.Records[0].FileID
        PID = TheindexFile.Records[0].PageID
        RID = TheindexFile.Records[0].RecordID

        newRecord = Record(FID, PID, RID, PrimaryKey)
        TheindexFile.Records.insert(index, newRecord)

        return (1,)
    FID = TheindexFile.Records[index - 1].FileID
    PID = TheindexFile.Records[index - 1].PageID
    RID = TheindexFile.Records[index - 1].RecordID
    newRecord = Record(FID, PID, RID, PrimaryKey)
    TheindexFile.Records.insert(index, newRecord)
    return index


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

    def __gt__(self, OtherRecord):
        return self.PrimaryKey < OtherRecord.PrimaryKey


class File:
    def __init__(
        self, isFull, NumberOfRecords, NumberOfPages, NextFile, PreviousFile, Pages
    ):
        self.Pages = Pages
        self.isFull = isFull
        self.NumberOfRecords = NumberOfRecords
        self.NumberOfPages = NumberOfPages
        self.NextFile = NextFile
        self.PreviousFile = PreviousFile


class Page:
    def __init__(self, NumberOfRecords, Records):
        self.NumberOfRecords = NumberOfRecords
        self.Records = Records


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
            self.NumberOfTypes = struct.unpack("i", self.SystemCatalogFile.read(4))[0]
            print(self.NumberOfTypes, " is read from SystemCatalog ")

            for i in range(self.NumberOfTypes):
                Type_Name = self.SystemCatalogFile.read(16).decode("utf-8")
                Type_Name = re.sub(" ", "", Type_Name)

                File_No = struct.unpack("b", self.SystemCatalogFile.read(1))[0]
                Fields_No = struct.unpack("b", self.SystemCatalogFile.read(1))[0]
                Fields = []
                for b in range(Fields_No):

                    tempx = self.SystemCatalogFile.read(16).decode("utf-8")
                    Fields.append(tempx)
                T = Type(Type_Name, File_No, Fields_No, Fields)
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
                filepath = "./indexFiles/" + str(Type.TypeName) + "index"
                print(filepath)
                TindexFile = open(filepath, "rb")
                NumberOfRecords = struct.unpack("i", TindexFile.read(4))[0]
                NumberOfRecordsPerFile = struct.unpack("i", TindexFile.read(4))[0]
                print("In related indexFile we found")
                print("The Number Of Records ", NumberOfRecords)
                print("The Number Of Records Per File", NumberOfRecordsPerFile)
                Temp_Records_Array = []
                for i in range(NumberOfRecords):
                    FileID = struct.unpack("b", TindexFile.read(1))[0]
                    PageID = struct.unpack("b", TindexFile.read(1))[0]
                    RecordID = struct.unpack("b", TindexFile.read(1))[0]
                    PrimaryKey = struct.unpack("i", TindexFile.read(4))[0]
                    Temp_Records_Array.append(
                        Record(int(FileID), int(PageID), int(RecordID), int(PrimaryKey))
                    )

                TI = classindexFile(
                    NumberOfRecords, NumberOfRecordsPerFile, Temp_Records_Array
                )

                self.indexFiles.update({Type.TypeName: TI})
                TindexFile.close()
            except Exception as e:
                print(e)

    def writebackindexFiles(self):
        print("All indexFiles are being written back...")
        for x in self.indexFiles:
            print("KEYS,", self.indexFiles[x])
        for key in self.indexFiles:
            filename = "./indexFiles/" + str(key) + "index"
            TindexFile = open(filename, "wb")
            TindexFile.write(struct.pack("i", self.indexFiles[key].Number_OF_Records))
            TindexFile.write(
                struct.pack("i", self.indexFiles[key].Max_Number_OF_Records_Per_File)
            )
            if len(self.indexFiles[key].Records) > 0:
                for record in self.indexFiles[key].Records:

                    TindexFile.write(bytes([int(record.FileID)]))
                    TindexFile.write(bytes([int(record.PageID)]))
                    TindexFile.write(bytes([int(record.RecordID)]))
                    TindexFile.write(struct.pack("i", record.PrimaryKey))

    def addNewType(self, Type):
        print("Increasing Number Of Files...")
        self.NumberOfTypes += 1
        print("Adding the new type to Types...")
        self.Types.append(Type)

    def complate(self, TypeName):
        for i in range(len(TypeName), 16):
            TypeName = " " + TypeName
        return TypeName

    def writeback(self):

        print("writing back on System Catalog File...")
        self.SystemCatalogFile = open("SystemCatalog", "wb")
        self.SystemCatalogFile.write(struct.pack("i", self.NumberOfTypes))
        for Type in self.Types:
            TypeName = self.complate(Type.TypeName)
            self.SystemCatalogFile.write(TypeName.encode("utf-8"))
            self.SystemCatalogFile.write(bytes([Type.NumberOfFiles]))
            self.SystemCatalogFile.write(bytes([Type.NumberOfFields]))

            for i in range(Type.NumberOfFields):
                FN = self.complate(Type.Fields_Names[i])
                self.SystemCatalogFile.write(FN.encode("utf-8"))

        self.SystemCatalogFile.close()


class DLL:
    def __init__(self, SystemCatalog):
        self.SystemCatalog = SystemCatalog

    def Create_Type(self, TypeName, NumberOfFields, Fields_Names):
        if Check_If_Type_Exits(TypeName, self.SystemCatalog) == None:

            # Creating First indexFile
            filename = "./indexFiles/" + str(TypeName) + "index"
            open(filename, "a").close()
            indexFile = open(filename, "wb")
            maxNoofRecordsPerFile = int(2048 / (4 * NumberOfFields)) * 256
            indexFile.write(struct.pack("i", 0))
            indexFile.write(struct.pack("i", maxNoofRecordsPerFile))
            indexFile.close()
            newType = Type(TypeName, 0, NumberOfFields, Fields_Names)
            self.SystemCatalog.addNewType(newType)

            self.SystemCatalog.indexFiles.update(
                {TypeName: classindexFile(0, maxNoofRecordsPerFile, [])}
            )

            # Creating First File
            os.mkdir("Files/" + str(TypeName))
            FirstFilePATH = "./Files/" + str(TypeName) + "/" + str(TypeName) + str(0)
            open(FirstFilePATH, "a").close()
            FirstFile = open(FirstFilePATH, "wb")
            FirstFile.write(struct.pack("i", 0))
            FirstFile.write(bytes([int(0)]))
            FirstFile.write(bytes([int(0)]))
            FirstFile.write(bytes([int(0)]))

        else:
            print("Type is not going to be written!!!!!!!")

    def Delete_Type(self):
        print()

    def List_All_Types(self):
        for i in range(self.SystemCatalog.NumberOfTypes):
            x = self.SystemCatalog.Types[i].TypeName


class DML:
    def __init__(self, SystemCatalog):
        self.SystemCatalog = SystemCatalog

    def Create_Record(self, TypeName, Fields_Values):

        filename = "./indexFiles/" + str(TypeName) + "index"
        indexFile = open(filename, "rb")
        indexFile.seek(4)

        MaxNumberOfRecordsPerFile = struct.unpack("i", indexFile.read(4))[0]

        MaxNumberOfRecordsPerPage = int(MaxNumberOfRecordsPerFile / 256)
        indexFile.seek(0, 0)
        main_indexFile = self.SystemCatalog.indexFiles[TypeName]
        Returned_Index = insert_Record_To_indexFile(
            Fields_Values[0], self.SystemCatalog.indexFiles[main_indexFile]
        )

        if Returned_Index != -1:
            index = Returned_Index
            while index < len(main_indexFile):
                main_indexFile.Records[index].RecordID += 1
                if main_indexFile.Records[index].RecordID == MaxNumberOfRecordsPerPage:
                    main_indexFile.Records[index].RecordID = 0


if not os.path.exists("indexFiles"):
    os.mkdir("indexFiles")

if not os.path.exists("Files"):
    os.mkdir("Files")


with SystemCatalog() as f:
    d1 = DLL(f)
    d2 = DML(f)
    d1.Create_Type(
        "Humans",
        10,
        ["age", "len", "spe", "age", "age", "age", "age", "age", "age", "age"],
    )  # 16
    d1.Create_Type("Cats", 4, ["age", "len", "spe", "smell"])  # 18
    d2.Create_Record("Humans", [1, 172, 45, 1, 1, 1, 2, 3, 4, 5])

"""
    for i in range(1, 51):
        
        print(
            f.indexFiles["Humans"].Records[i].FileID,
            f.indexFiles["Humans"].Records[i].PageID,
            f.indexFiles["Humans"].Records[i].RecordID,
            f.indexFiles["Humans"].Records[i].PrimaryKey,
        )
"""
