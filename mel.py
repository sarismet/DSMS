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
    MutualPath = "Files/"+str(TypeName)+"/"+str(TypeName)
    FirstFile = open(MutualPath+str(Previous), "wb")
    SecondFile = open(MutualPath+str(Next), "wb")
    FirstFile.seek(6)
    SecondFile.seek(7)
    FirstFile.write(bytes([int(Next)]))
    SecondFile.write(bytes([int(Previous)]))


def binarySearch(arr, l, r, x):
    #print(r, l)
    # Check base case
    if r > l:

        mid = l + (r - l) // 2

        # If element is present at the middle itself
        if arr[mid].PrimaryKey > x and arr[mid-1].PrimaryKey < x:
            return (1, mid)

        # If element is smaller than mid, then it
        # can only be present in left subarray
        elif arr[mid].PrimaryKey > x and arr[mid-1].PrimaryKey > x:
            return binarySearch(arr, l, mid-1, x)

        # Else the element can only be present
        # in right subarray
        else:
            return binarySearch(arr, mid + 1, r, x)
    elif r == 0 and l == 0:
        if arr[r].PrimaryKey > x:
            return (0, 0)
        else:
            return (1, 1)
    elif r == len(arr)-1 and l == len(arr)-1:
        # Element is not present in the array

        return (1, len(arr)-1)


def insert_Record_To_indexFile(PrimaryKey, TheindexFile):
    index = 0

    """for RecordN in TheindexFile.Records:
        # BINARYYYYYYYYYY SEARCHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH
        if RecordN.PrimaryKey > PrimaryKey:

            break
        index += 1"""
    index = binarySearch(TheindexFile.Records, 0, len(
        TheindexFile.Records)-1, PrimaryKey)
    print("Returned index", index)

    RID = TheindexFile.Records[index].RecordID
    FID = TheindexFile.Records[index].FileID
    PID = TheindexFile.Records[index].PageID
    border = int(TheindexFile.Max_Number_OF_Records_Per_File/256)

    if RID > border-1:
        RID = 0
        PID += 1
    if PID == 256:
        PID = 0
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

    def __gt__(self, OtherRecord):
        return self.PrimaryKey < OtherRecord.PrimaryKey


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
            maxNoofRecords = int(2048/(4*NumberOfFields))*256
            indexFile.write(struct.pack("i", 0))
            indexFile.write(struct.pack("i", maxNoofRecords))
            indexFile.close()
            newType = Type(TypeName, 0, NumberOfFields, Fields_Names)
            self.SystemCatalog.addNewType(newType)

            self.SystemCatalog.indexFiles.update(
                {TypeName: classindexFile(0, maxNoofRecords, [])})

            if not os.path.exists("Files/"+str(TypeName)):
                os.mkdir("Files/"+str(TypeName))
            FirstFilePATH = "./Files/" + \
                str(TypeName)+"/"+str(TypeName)+str(0)

            if not os.path.exists(FirstFilePATH):
                open(FirstFilePATH, "a").close()

            FirstFile = open(FirstFilePATH, "wb")
            FirstFile.write(bytes([int(0)]))
            FirstFile.write(struct.pack("i", 0))
            FirstFile.write(bytes([int(0)]))
            FirstFile.write(bytes([int(0)]))
            FirstFile.write(bytes([int(0)]))

        else:
            print("Type is not going to be written!!!!!!!")

    def List_All_Types(self):
        for i in range(self.SystemCatalog.NumberOfTypes):
            x = self.SystemCatalog.Types[i].TypeName


class DML:

    def __init__(self, SystemCatalog):
        self.SystemCatalog = SystemCatalog

    def Create_Record(self, TypeName, Fields_Values):

        filename = "./indexFiles/"+str(TypeName)+"index"
        indexFile = open(filename, "rb")
        indexFile.seek(4)
        if len(self.SystemCatalog.indexFiles[str(TypeName)].Records) == 0:
            self.SystemCatalog.indexFiles[TypeName].Records.append(
                Record(0, 0, 0, Fields_Values[0]))
        MaxNumberOfRecordsPerFile = struct.unpack(
            "i", indexFile.read(4))[0]

        MaxNumberOfRecordsPerPage = int(MaxNumberOfRecordsPerFile/256)
        indexFile.seek(0, 0)
        TuppleReturned = insert_Record_To_indexFile(
            Fields_Values[0], self.SystemCatalog.indexFiles[TypeName])

        File = None
        PATH = "./Files/" + \
            str(TypeName)+"/"+str(TypeName)+str(TuppleReturned[1].FileID)
        if not os.path.exists(PATH):
            open(PATH, "a").close()
            LinkFilesbetween(
                TypeName, TuppleReturned[1].FileID-1, TuppleReturned[1].FileID)


if not os.path.exists("indexFiles"):
    os.mkdir("indexFiles")

if not os.path.exists("Files"):
    os.mkdir("Files")


with SystemCatalog() as f:
    d1 = DLL(f)
    d2 = DML(f)
    d1.Create_Type("Humans", 10, [
                   "age", "len", "spe", "age", "age", "age", "age", "age", "age", "age"])  # 16
    d1.Create_Type("Cats", 4, ["age", "len", "spe", "smell"])  # 18

    for i in range(1, 51):
        d2.Create_Record("Humans", [i, 172, 45, 1, 1, 1, 2, 3, 4, 5])
        print(f.indexFiles["Humans"].Records[i].FileID, f.indexFiles["Humans"].Records[i].PageID,
              f.indexFiles["Humans"].Records[i].RecordID, f.indexFiles["Humans"].Records[i].PrimaryKey)
