import struct
import re
import os
import sys
import shutil
from random import randrange

# We know that the input will be correct but in any case we define an exception to not be crashed

# this exception is designed if the record we are looking for is not found
class RecordisNotFound(Exception):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)

# this exception is designed if the type we are looking for is not found
class TypeDoesNotExists(Exception):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)

#Here Creating Type object
class Type:
    def __init__(self, TypeName, NumberOfFiles, NumberOfFields, Fields_Names, Files):

        self.TypeName = TypeName
        self.NumberOfFiles = NumberOfFiles
        self.NumberOfFields = NumberOfFields
        self.Fields_Names = Fields_Names
        self.Files = Files

    def addFile(self, newFile):
        self.Files.append(newFile)
        self.NumberOfFiles += 1

#Here Creating File object
class File:
    def __init__(self, NumberOfRecords, NumberOfPages, NextFile, PreviousFile, Pages):

        self.NumberOfRecords = NumberOfRecords
        self.Pages = Pages
        self.NumberOfPages = NumberOfPages
        self.NextFile = NextFile
        self.PreviousFile = PreviousFile

    def addPage(self, newPage):
        self.Pages.append(newPage)
        self.NumberOfPages += 1

#Here Creating Page object
class Page:
    def __init__(self, NumberOfRecords, Records):
        self.NumberOfRecords = NumberOfRecords
        self.Records = Records

    def addPage(self, newRecord):
        self.Records.append(newRecord)
        self.NumberOfRecords += 1

#Here Creating Record object
class Record:
    def __init__(self, Fields):
        self.Fields = Fields

    def Update_Fields(self, newFields):
        self.Fields = newFields

#Here Creating indexFile object
class indexFile:
    def __init__(self, Number_OF_Records, Max_Number_OF_Records_Per_File, Records):
        self.Number_OF_Records = Number_OF_Records
        self.Max_Number_OF_Records_Per_File = Max_Number_OF_Records_Per_File
        self.Records = Records

    def add_new_indexFile_Record(self, new_indexFile_Record):
        self.Records.append(new_indexFile_Record)
        self.Number_OF_Records += 1

#Here Creating Record_indexFile object. This is different from the Record in that this is used in
# indexFile and it has different fields.
class Record_indexFile:
    def __init__(self, FileID, PageID, RecordID, PrimaryKey):
        self.FileID = FileID
        self.PageID = PageID
        self.RecordID = RecordID
        self.PrimaryKey = PrimaryKey

#This is SystemCatalog. We read and write back all the processed datas on files here.
class SystemCatalog:
    def __init__(self):
        # a system catalog can only store 2^32 - 1 type as I mention pdf file
        self.NumberOfTypes = 0
        self.Types = {}
        self.indexFiles = {}
        self.SystemCatalogFile = None

    # when we open system catalog file first it will be executed.
    def __enter__(self):
        if os.path.exists("SystemCatalog"):
            print("SystemCatalogFile is opened successsively")
            # if there is a system catalog which means that we executed one test cases
            # then it will read the system catalog
            self.readSystemCatalogFile()
        else:
            # if there is no system catalog then it will first create it
            print("There is no such a file...")
            self.createCatalogFile()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        print("Closing System Catalog...")

        self.writeback()

    def createCatalogFile(self):
        print("Creating a new SystemCatalog File...")
        open("SystemCatalog", "a").close()

    def readSystemCatalogFile(self):
        print("SystemCatalogFile is being read...")
        try:
            self.SystemCatalogFile = open("SystemCatalog", "rb")
            self.NumberOfTypes = struct.unpack(
                "i", self.SystemCatalogFile.read(4))[0]
            print("There are ", self.NumberOfTypes, " Number Of Types.")

            for i in range(self.NumberOfTypes):

                Temp_Type_Name = self.SystemCatalogFile.read(
                    16).decode("utf-8")
                Type_Name = re.sub(" ", "", Temp_Type_Name)
                # print("Type_Name",Type_Name)
                File_No = struct.unpack("B", self.SystemCatalogFile.read(1))[0]
                Fields_No = struct.unpack(
                    "B", self.SystemCatalogFile.read(1))[0]
                Fields = []
                # print("Fields_No",Fields_No)
                for b in range(Fields_No):

                    tempx = self.SystemCatalogFile.read(16).decode("utf-8")
                    tempx = re.sub(" ", "", tempx)
                    Fields.append(tempx)
                Files = []
                print("Fields", Fields)
                for c in range(int(File_No)):

                    #print("Openned file ","Files/" + str(Type_Name) + "/" +str(Type_Name) + str(c))

                    FileOpen = open(
                        "Files/" + str(Type_Name) + "/" +
                        str(Type_Name) + str(c), "rb"
                    )

                    NumberOfRecordsinFileHeader = struct.unpack("i", FileOpen.read(4))[
                        0
                    ]
                    # print("NumberOfRecordsinFileHeader",NumberOfRecordsinFileHeader)
                    NumberOfPageinFileHeader = struct.unpack(
                        "B", FileOpen.read(1))[0]
                    # print("NumberOfPageinFileHeader",NumberOfPageinFileHeader)
                    NextFileinFileHeader = struct.unpack(
                        "B", FileOpen.read(1))[0]
                    PreviousFileinFileHeader = struct.unpack(
                        "B", FileOpen.read(1))[0]

                    Pages = []

                    for d in range(int(NumberOfPageinFileHeader)):

                        Records = []
                        NumberOfRecordsinPageHeader = struct.unpack(
                            "B", FileOpen.read(1))[0]
                        bandwidth = NumberOfRecordsinPageHeader*4*Fields_No

                        AllPage = FileOpen.read(bandwidth)

                        indexN = 0

                        #print("NumberOfRecords in Page ",d," = ",NumberOfRecordsinPageHeader)
                        Record_No = 0
                        while indexN < (NumberOfRecordsinPageHeader*4*Fields_No):
                            #print("Reading Record ",Record_No,end=" ")
                            Fields_Of_NewRecord = []
                            for f in range(Fields_No):
                                #print("indexN and indexN+4",indexN,indexN+4,end=" ")
                                Fields_Of_NewRecord.append(
                                    int(struct.unpack(
                                        "i", AllPage[indexN:indexN+4])[0]))
                                indexN = indexN+4
                            Records.append(Record(Fields_Of_NewRecord))
                            #print("Fields ",Fields_Of_NewRecord)
                            Record_No += 1

                        Pages.append(
                            Page(NumberOfRecordsinPageHeader, Records))

                    Files.append(
                        File(
                            NumberOfRecordsinFileHeader,
                            NumberOfPageinFileHeader,
                            NextFileinFileHeader,
                            PreviousFileinFileHeader,
                            Pages,
                        )
                    )
                    FileOpen.close()

                T = Type(Type_Name, File_No, Fields_No, Fields, Files)
                self.Types.update({str(Type_Name): T})
            self.SystemCatalogFile.close()
            print("Reading All Type Files is Compleated")

            print("Phase 2 ....Reading All indexFiles.... ")
            self.readindexFiles()
        except Exception as e:
            print(e)
            self.SystemCatalogFile.close()

    def readindexFiles(self):
        print("All indexFiles are being read...")
        for TypeName in self.Types:
            try:
                filepath = "./indexFiles/" + str(TypeName) + "index"

                TindexFile = open(filepath, "rb")
                NumberOfRecords = struct.unpack("i", TindexFile.read(4))[0]
                NumberOfRecordsPerFile = struct.unpack(
                    "i", TindexFile.read(4))[0]

                Temp_Records_Array = []
                for i in range(NumberOfRecords):
                    FileID = struct.unpack("B", TindexFile.read(1))[0]
                    PageID = struct.unpack("B", TindexFile.read(1))[0]
                    RecordID = struct.unpack("B", TindexFile.read(1))[0]
                    PrimaryKey = struct.unpack("i", TindexFile.read(4))[0]
                    Temp_Records_Array.append(
                        Record_indexFile(
                            int(FileID), int(PageID), int(
                                RecordID), int(PrimaryKey)
                        )
                    )

                TI = indexFile(
                    NumberOfRecords, NumberOfRecordsPerFile, Temp_Records_Array
                )

                self.indexFiles.update({TypeName: TI})
                TindexFile.close()
                print("Reading All  indexFiles is Compleated")
            except Exception as e:
                print(e)

    def writeback(self):

        print("writing back on System Catalog File...")
        try:
            self.SystemCatalogFile = open("SystemCatalog", "wb")
            self.SystemCatalogFile.write(struct.pack("i", self.NumberOfTypes))
            for TypeName in self.Types:
                TypeNameToWrite = self.complate(TypeName)
                self.SystemCatalogFile.write(TypeNameToWrite.encode("utf-8"))
                self.SystemCatalogFile.write(
                    bytes([self.Types[TypeName].NumberOfFiles])
                )
                self.SystemCatalogFile.write(
                    bytes([self.Types[TypeName].NumberOfFields])
                )

                for i in range(self.Types[TypeName].NumberOfFields):
                    FN = self.complate(self.Types[TypeName].Fields_Names[i])
                    self.SystemCatalogFile.write(FN.encode("utf-8"))
                    index = 0
                for File in self.Types[TypeName].Files:
                    PATH = "Files/" + str(TypeName) + \
                        "/" + str(TypeName) + str(index)

                    if not os.path.exists(PATH):
                        print("Creating a new Type-File")
                        open(PATH, "a").close()
                    File_To_Write = open(PATH, "wb")

                    File_To_Write.write(struct.pack("i", File.NumberOfRecords))

                    File_To_Write.write(bytes([File.NumberOfPages]))
                    File_To_Write.write(bytes([File.NextFile]))
                    File_To_Write.write(bytes([File.PreviousFile]))
                    try:
                        for Page in File.Pages:

                            File_To_Write.write(bytes([Page.NumberOfRecords]))
                            for Record in Page.Records:
                                for Field in Record.Fields:
                                    File_To_Write.write(
                                        struct.pack("i", Field))

                    except Exception as e:
                        print(e)
                    File_To_Write.close()
                    index += 1
            self.SystemCatalogFile.close()
            print("Writing of All Type Files is compleated!!!")
            self.writebackindexFiles()

        except Exception as e:
            print("There is an exception occured in writing back such that \n ", e)

    def writebackindexFiles(self):
        print("All indexFiles are being written back...")

        for key in self.indexFiles:
            filename = "./indexFiles/" + str(key) + "index"
            TindexFile = open(filename, "wb")
            TindexFile.write(struct.pack(
                "i", self.indexFiles[key].Number_OF_Records))
            TindexFile.write(
                struct.pack(
                    "i", self.indexFiles[key].Max_Number_OF_Records_Per_File)
            )
            if len(self.indexFiles[key].Records) > 0:
                for record in self.indexFiles[key].Records:

                    TindexFile.write(bytes([int(record.FileID)]))
                    TindexFile.write(bytes([int(record.PageID)]))
                    TindexFile.write(bytes([int(record.RecordID)]))
                    TindexFile.write(struct.pack("i", record.PrimaryKey))

        print("Writing of All indexFiles is compleated!!!")

    def complate(self, TypeName):
        for i in range(len(TypeName), 16):
            TypeName = " " + TypeName
        return TypeName


class DLL:
    def __init__(self, SystemCatalog):
        self.SystemCatalog = SystemCatalog

    def Create_Type(self, TypeName, NumberOfFields, Fields_Names):

        newType = Type(TypeName, 0, NumberOfFields, Fields_Names, [])
        self.SystemCatalog.Types.update({TypeName: newType})

        self.SystemCatalog.NumberOfTypes += 1
        PATH = "./indexFiles/" + str(TypeName) + "index"
        if not os.path.exists(PATH):
            os.mkdir("Files/" + str(TypeName))

        open(PATH, "a").close()
        maxNoofRecordsPerFile = int(2048 / (4 * NumberOfFields)) * 255
        newIndexFile = indexFile(0, maxNoofRecordsPerFile, [])
        self.SystemCatalog.indexFiles.update({TypeName: newIndexFile})
        print("The operation of creating type is compleated.")

    def Delete_Type(self, TypeName):

        print("Deleting Type -", TypeName)
        print(str(TypeName) + "index")
        del self.SystemCatalog.Types[TypeName]
        self.SystemCatalog.NumberOfTypes -= 1
        del self.SystemCatalog.indexFiles[TypeName]
        PATHindexFile = "indexFiles/" + str(TypeName) + "index"
        PATHtype = "Files/" + str(TypeName)
        if os.path.exists(PATHtype):
            try:
                os.system("rm ./indexFiles/" + str(TypeName) + "index")
                os.system("rm -rf ./Files/" + str(TypeName))
                print("The operation of deleting type is compleated.")
            except:
                print("Invalid input")
        else:
            raise TypeDoesNotExists(
                "Type'path does not exist so we cannot delete")

    def List_All_Types(self, outputFile):

        sorted_list_of_All_Types = sorted(self.SystemCatalog.Types)
        for Type in sorted_list_of_All_Types:
            print(Type)
            outputFile.write(str(Type)+"\n")


class DML:
    def __init__(self, SystemCatalog):
        self.SystemCatalog = SystemCatalog

    def Create_Record(self, TypeName, Fields_Values):
        Type = self.SystemCatalog.Types[TypeName]

        indexFile = self.SystemCatalog.indexFiles[TypeName]

        MaxNumberOfRecordsPerFile = indexFile.Max_Number_OF_Records_Per_File

        MaxNumberOfRecordsPerPage = int(MaxNumberOfRecordsPerFile / 255)

        main_indexFile = self.SystemCatalog.indexFiles[TypeName]
        newRecord = Record(Fields_Values)
        if len(main_indexFile.Records) == 0:
            newRecord = Record_indexFile(0, 0, 0, Fields_Values[0])
            main_indexFile.add_new_indexFile_Record(newRecord)
            newPage = Page(1, [Record(Fields_Values)])

            newFile = File(1, 1, 255, 255, [newPage])

            self.SystemCatalog.Types[TypeName].addFile(newFile)
            return

        Returned_Index = insert_Record_To_indexFile(
            Fields_Values, main_indexFile, Type, MaxNumberOfRecordsPerPage
        )

        index = Returned_Index
        while index < main_indexFile.Number_OF_Records:

            main_indexFile.Records[index].RecordID += 1
            if main_indexFile.Records[index].RecordID == MaxNumberOfRecordsPerPage:
                main_indexFile.Records[index].RecordID = 0
                main_indexFile.Records[index].PageID += 1

                if main_indexFile.Records[index].PageID == 255:

                    main_indexFile.Records[index].PageID = 0
                    main_indexFile.Records[index].FileID += 1
                    if main_indexFile.Records[index].FileID == len(Type.Files):
                        preFileID = main_indexFile.Records[index].FileID - 1
                        newFile = File(1, 1, 255, preFileID, [])
                        Type.Files[main_indexFile.Records[index].FileID -
                                   1].NextFile = main_indexFile.Records[index].FileID

                        newPage = Page(1, [])
                        newRecord = (
                            Type.Files[main_indexFile.Records[index].FileID - 1]
                            .Pages[254]
                            .Records.pop()
                        )
                        newPage.Records.append(newRecord)
                        newFile.Pages.append(newPage)
                        Type.addFile(newFile)
                        return
                    if (
                        Type.Files[main_indexFile.Records[index].FileID].NumberOfRecords
                        < MaxNumberOfRecordsPerPage * 255
                    ):
                        if (
                            Type.Files[main_indexFile.Records[index].FileID]
                            .Pages[0]
                            .NumberOfRecords
                            < MaxNumberOfRecordsPerPage
                        ):

                            Type.Files[main_indexFile.Records[index].FileID].Pages[
                                0
                            ].NumberOfRecords += 1
                        Type.Files[
                            main_indexFile.Records[index].FileID
                        ].NumberOfRecords += 1

                    newRecord = (
                        Type.Files[main_indexFile.Records[index].FileID - 1]
                        .Pages[254]
                        .Records.pop()
                    )
                    Type.Files[main_indexFile.Records[index].FileID].Pages[
                        0
                    ].Records.insert(0, newRecord)
                    index += 1
                    continue
                if (
                    Type.Files[main_indexFile.Records[index].FileID].NumberOfPages
                    == main_indexFile.Records[index].PageID
                ):
                    newPage = Page(0, [])
                    Type.Files[main_indexFile.Records[index].FileID].addPage(
                        newPage)

                if (
                    Type.Files[main_indexFile.Records[index].FileID]

                    .Pages[main_indexFile.Records[index].PageID].NumberOfRecords
                    < MaxNumberOfRecordsPerPage
                ):
                    Type.Files[main_indexFile.Records[index].FileID].Pages[
                        main_indexFile.Records[index].PageID
                    ].NumberOfRecords += 1
                newRecord = (
                    Type.Files[main_indexFile.Records[index].FileID]
                    .Pages[main_indexFile.Records[index].PageID - 1]
                    .Records.pop()
                )
                Type.Files[main_indexFile.Records[index].FileID].Pages[
                    main_indexFile.Records[index].PageID
                ].Records.insert(0, newRecord)

            index += 1

    def Delete_Record(self, TypeName, PrimaryKey):

        Returned_Tupple = FindRecord(
            self.SystemCatalog.indexFiles[TypeName].Records,
            0,
            len(self.SystemCatalog.indexFiles[TypeName].Records) - 1,
            PrimaryKey,
        )
        Record = Returned_Tupple[0]
        FileID = Record.FileID
        PageID = Record.PageID
        RecordID = Record.RecordID

        del (
            self.SystemCatalog.Types[TypeName]
            .Files[FileID]
            .Pages[PageID]
            .Records[RecordID]
        )

        self.SystemCatalog.indexFiles[TypeName].Number_OF_Records -= 1
        del self.SystemCatalog.indexFiles[TypeName].Records[Returned_Tupple[1]]
        index = Returned_Tupple[1]

        MaxNumberOfRecordsPerFile = self.SystemCatalog.indexFiles[
            TypeName].Max_Number_OF_Records_Per_File
        MaxNumberOfRecordsPerPage = int(MaxNumberOfRecordsPerFile / 255)
        main_indexFile = self.SystemCatalog.indexFiles[TypeName]

        while index < self.SystemCatalog.indexFiles[TypeName].Number_OF_Records:

            main_indexFile.Records[index].RecordID -= 1
            if main_indexFile.Records[index].RecordID == -1:
                main_indexFile.Records[index].RecordID = MaxNumberOfRecordsPerPage - 1

                main_indexFile.Records[index].PageID -= 1

                if main_indexFile.Records[index].PageID == -1:

                    main_indexFile.Records[index].PageID = 254
                    main_indexFile.Records[index].FileID -= 1

                    Record_poped = (
                        self.SystemCatalog.Types[TypeName]
                        .Files[main_indexFile.Records[index].FileID + 1]
                        .Pages[0]
                        .Records.pop(0)
                    )

                    self.SystemCatalog.Types[TypeName].Files[main_indexFile.Records[index].FileID].Pages[254].Records.append(
                        Record_poped)

                elif main_indexFile.Records[index].PageID >= 0:

                    Record_poped = (
                        self.SystemCatalog.Types[TypeName]
                        .Files[main_indexFile.Records[index].FileID]
                            .Pages[main_indexFile.Records[index].PageID+1]
                            .Records.pop(0)
                    )

                    self.SystemCatalog.Types[TypeName].Files[main_indexFile.Records[index]
                                                             .FileID].Pages[main_indexFile.Records[index].PageID].Records.append(Record_poped)

            index += 1

        indexF = len(f.Types[TypeName].Files)-1

        while indexF >= 0:

            indexP = f.Types[TypeName].Files[indexF].NumberOfPages-1
            NumberOfRecordOfFile = 0
            flag_page = False
            while indexP >= 0:
                if flag_page == False:
                    NumberOfRecordOfPage = len(
                        f.Types[TypeName].Files[indexF].Pages[indexP].Records)
                    NumberOfRecordOfFile = NumberOfRecordOfFile+NumberOfRecordOfPage
                    if NumberOfRecordOfPage == 0:
                        del f.Types[TypeName].Files[indexF].Pages[indexP]
                    else:
                        flag_page = True
                        f.Types[TypeName].Files[indexF].Pages[indexP].NumberOfRecords = NumberOfRecordOfPage
                elif flag_page == True:
                    NumberOfRecordOfFile += MaxNumberOfRecordsPerPage
                    f.Types[TypeName].Files[indexF].Pages[indexP].NumberOfRecords = MaxNumberOfRecordsPerPage
                indexP -= 1

            if NumberOfRecordOfFile == 0:
                if f.Types[TypeName].Files[indexF].PreviousFile != 255:
                    f.Types[TypeName].Files[f.Types[TypeName]
                                            .Files[indexF].PreviousFile].NextFile = 255
                del f.Types[TypeName].Files[indexF]
                PATH = "Files/"+str(TypeName)+"/"+str(TypeName)+str(indexF)
                if os.path.exists(PATH):
                    os.remove(PATH)
            else:
                f.Types[TypeName].Files[indexF].NumberOfRecords = NumberOfRecordOfFile
                f.Types[TypeName].Files[indexF].NumberOfPages = len(
                    f.Types[TypeName].Files[indexF].Pages)

            indexF -= 1

        f.Types[TypeName].NumberOfFiles = len(f.Types[TypeName].Files)

    def Update_Record(self, TypeName, new_Fields_Values):
        Record = FindRecord(
            self.SystemCatalog.indexFiles[TypeName].Records,
            0,
            len(self.SystemCatalog.indexFiles[TypeName].Records) - 1,
            new_Fields_Values[0],
        )[0]
        FileID = Record.FileID
        PageID = Record.PageID
        RecordID = Record.RecordID
        print(
            "Before : ",
            self.SystemCatalog.Types[TypeName]
            .Files[FileID]
            .Pages[PageID]
            .Records[RecordID]
            .Fields,
        )
        self.SystemCatalog.Types[TypeName].Files[FileID].Pages[PageID].Records[
            RecordID
        ].Update_Fields(new_Fields_Values)
        print(
            "After : ",
            self.SystemCatalog.Types[TypeName]
            .Files[FileID]
            .Pages[PageID]
            .Records[RecordID]
            .Fields,
        )
        print("Update is completed")

    def Search_Record(self, TypeName, PrimaryKey, outputFile):
        Record = FindRecord(
            self.SystemCatalog.indexFiles[TypeName].Records,
            0,
            len(self.SystemCatalog.indexFiles[TypeName].Records) - 1,
            PrimaryKey,
        )[0]
        FileID = Record.FileID
        PageID = Record.PageID
        RecordID = Record.RecordID
        Fields = (
            self.SystemCatalog.Types[TypeName]
            .Files[FileID]
            .Pages[PageID]
            .Records[RecordID]
            .Fields
        )
        i = 0
        for Field in Fields:
            print(i, "th Field is : ", Field, " ", end="")
            outputFile.write(str(Field)+" ")
            i += 1
        outputFile.write("\n")

    def List_Records(self, TypeName, outputFile):

        for File in self.SystemCatalog.Types[TypeName].Files:
            for Page in File.Pages:
                for Record in Page.Records:
                    for Field in Record.Fields:
                        print("th Field is : ", Field, " ", end="")
                        outputFile.write(str(Field)+" ")
                    outputFile.write("\n")


def FindRecord(arr, l, r, x):

    # Check base case
    if r >= l:

        mid = l + (r - l) // 2

        # If element is present at the middle itself
        if arr[mid].PrimaryKey == x:
            return (arr[mid], mid)

        # If element is smaller than mid, then it can only
        # be present in left subarray
        elif arr[mid].PrimaryKey > x:
            return FindRecord(arr, l, mid - 1, x)

        # Else the element can only be present in right subarray
        else:
            return FindRecord(arr, mid + 1, r, x)

    else:
        # Element is not present in the array
        raise RecordisNotFound("Primary Key : ", x, " is not found")


def FindPlaceOfRecord(arr, l, r, x):

    if r == l:
        if arr[r].PrimaryKey > x:
            # arr.insert(r, x)
            return r

        else:
            # arr.insert(r + 1, x)
            return r + 1
    elif (r - l) == 1:
        if arr[l].PrimaryKey < x and arr[r].PrimaryKey > x:
            # arr.insert(l, x)
            return r
        elif arr[l].PrimaryKey > x:
            return FindPlaceOfRecord(arr, l, l, x)
        elif arr[r].PrimaryKey < x:
            return FindPlaceOfRecord(arr, r, r, x)
    elif r >= l:

        mid = l + (r - l) // 2

        if arr[mid - 1].PrimaryKey < x and arr[mid].PrimaryKey > x:
            # arr.insert(mid, x)
            return mid

        elif arr[mid - 1].PrimaryKey > x:
            return FindPlaceOfRecord(arr, l, mid - 1, x)

        # Else the element can only be present in right subarray
        elif arr[mid].PrimaryKey < x:
            return FindPlaceOfRecord(arr, mid, r, x)


def insert_Record_To_indexFile(Fields, TheindexFile, Type, MaxNumberOfRecordsPerPage):
    TheindexFile.Number_OF_Records += 1

    index = 0
    PrimaryKey = Fields[0]

    index = FindPlaceOfRecord(
        TheindexFile.Records, 0, TheindexFile.Number_OF_Records - 2, PrimaryKey
    )

    if index == 0:

        newRecord = Record_indexFile(0, 0, 0, PrimaryKey)
        TheindexFile.Records.insert(index, newRecord)
        Type.Files[0].Pages[0].Records.insert(0, Record(Fields))
        if Type.Files[0].NumberOfRecords < 255 * MaxNumberOfRecordsPerPage:
            if Type.Files[0].Pages[0].NumberOfRecords < MaxNumberOfRecordsPerPage:
                Type.Files[0].Pages[0].NumberOfRecords += 1
            Type.Files[0].NumberOfRecords += 1

        return 1
    FID = TheindexFile.Records[index - 1].FileID
    PID = TheindexFile.Records[index - 1].PageID
    RID = TheindexFile.Records[index - 1].RecordID
    newRecord = Record_indexFile(FID, PID, RID, PrimaryKey)
    TheindexFile.Records.insert(index, newRecord)
    Type.Files[FID].Pages[PID].Records.insert(RID + 1, Record(Fields))

    if Type.Files[FID].NumberOfRecords < 255 * MaxNumberOfRecordsPerPage:
        if Type.Files[FID].Pages[PID].NumberOfRecords < MaxNumberOfRecordsPerPage:
            Type.Files[FID].Pages[PID].NumberOfRecords += 1
        Type.Files[FID].NumberOfRecords += 1
    return index


inputFile = ""
outputFile = ""

if(len(sys.argv) > 1):
    inputFile = str(sys.argv[1])
    outputFile = str(sys.argv[2])

if not os.path.exists("indexFiles"):
    os.mkdir("indexFiles")

if not os.path.exists("Files"):
    os.mkdir("Files")

with SystemCatalog() as f:

    d1 = DLL(f)

    d2 = DML(f)

    ReadingInputFile = open(inputFile, "r")
    Lines = ReadingInputFile.readlines()

    WritingOnOutFile = open(outputFile, "w")

    for line in Lines:
        lists = line.strip().split(" ")
        Command = lists[0]+" "+lists[1]
        if Command == "create type":
            print("The command is create type")
            try:
                d1.Create_Type(lists[2], int(lists[3]), lists[4:])
            except:
                print("In valid input")
        elif Command == "delete type":
            print("The command is delete type")
            try:
                d1.Delete_Type(lists[2])
            except:
                print("In valid input")
        elif Command == "list type":
            print("The command is list type")
            try:
                d1.List_All_Types(WritingOnOutFile)
            except:
                print("In valid input")
        elif Command == "create record":
            print("The command is create record")
            try:
                Fields = [int(i) for i in lists[3:]]
                d2.Create_Record(lists[2], Fields)
            except:
                print("In valid input")
        elif Command == "delete record":
            print("The command is delete record")
            try:
                d2.Delete_Record(lists[2], int(lists[3]))
            except:
                print("In valid input")
        elif Command == "update record":
            print("The command is update record")
            try:
                Fields = [int(i) for i in lists[3:]]
                d2.Update_Record(lists[2], Fields)
            except:
                print("In valid input")
        elif Command == "search record":
            print("The command is search record")
            try:
                d2.Search_Record(lists[2], int(lists[3]), WritingOnOutFile)
            except:
                print("In valid input")
        elif Command == "list record":
            print("The command is list record")
            try:
                d2.List_Records(lists[2], WritingOnOutFile)
            except:
                print("In valid input")

    ReadingInputFile.close()
    WritingOnOutFile.close()
