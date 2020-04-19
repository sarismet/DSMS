import struct
import re
import os
import shutil
from random import randrange

class FieldsAreNotValid(Exception):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)

class TypeAlreadyExists(Exception):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)

class RecordisNotFound(Exception):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)

class TypeDoesNotExists(Exception):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)


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


class Page:
    def __init__(self, NumberOfRecords, Records):
        self.NumberOfRecords = NumberOfRecords
        self.Records = Records

    def addPage(self, newRecord):
        self.Records.append(newRecord)
        self.NumberOfRecords += 1


class Record:
    def __init__(self, Fields):
        self.Fields = Fields

    def Update_Fields(self, newFields):
        self.Fields = newFields


class indexFile:
    def __init__(self, Number_OF_Records, Max_Number_OF_Records_Per_File, Records):
        self.Number_OF_Records = Number_OF_Records
        self.Max_Number_OF_Records_Per_File = Max_Number_OF_Records_Per_File
        self.Records = Records

    def add_new_indexFile_Record(self, new_indexFile_Record):
        self.Records.append(new_indexFile_Record)
        self.Number_OF_Records += 1


class Record_indexFile:
    def __init__(self, FileID, PageID, RecordID, PrimaryKey):
        self.FileID = FileID
        self.PageID = PageID
        self.RecordID = RecordID
        self.PrimaryKey = PrimaryKey


class SystemCatalog:
    def __init__(self):
        self.NumberOfTypes = 0
        self.Types = {}
        self.indexFiles = {}
        self.SystemCatalogFile = None

    def __enter__(self):
        if os.path.exists("SystemCatalog"):
            print("SystemCatalogFile is opened successsively")
            self.readSystemCatalogFile()
        else:

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
                #print("Type_Name",Type_Name)
                File_No = struct.unpack("B", self.SystemCatalogFile.read(1))[0]
                Fields_No = struct.unpack(
                    "B", self.SystemCatalogFile.read(1))[0]
                Fields = []
                #print("Fields_No",Fields_No)
                for b in range(Fields_No):

                    tempx = self.SystemCatalogFile.read(16).decode("utf-8")
                    tempx = re.sub(" ", "", tempx)
                    Fields.append(tempx)
                Files = []
                print("Fields",Fields)
                for c in range(int(File_No)):   

                    #print("Openned file ","Files/" + str(Type_Name) + "/" +str(Type_Name) + str(c))

                    FileOpen = open(
                        "Files/" + str(Type_Name) + "/" +str(Type_Name) + str(c), "rb"
                    )

                    NumberOfRecordsinFileHeader = struct.unpack("i", FileOpen.read(4))[
                        0
                    ]
                    #print("NumberOfRecordsinFileHeader",NumberOfRecordsinFileHeader)
                    NumberOfPageinFileHeader = struct.unpack(
                        "B", FileOpen.read(1))[0]
                    #print("NumberOfPageinFileHeader",NumberOfPageinFileHeader)
                    NextFileinFileHeader = struct.unpack(
                        "B", FileOpen.read(1))[0]
                    PreviousFileinFileHeader = struct.unpack(
                        "B", FileOpen.read(1))[0]
                    
                    Pages = []
                    maxRecordNo = int(2048 / (4 * Fields_No))

                    for d in range(int(NumberOfPageinFileHeader)):

                        Records = []
                        NumberOfRecordsinPageHeader= struct.unpack(
                        "B", FileOpen.read(1))[0]
                        bandwidth = NumberOfRecordsinPageHeader*4*Fields_No

                        AllPage = FileOpen.read(bandwidth)
                        
                        indexN = 0
                        
                        #print("NumberOfRecords in Page ",d," = ",NumberOfRecordsinPageHeader)
                        Record_No=0
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
                            Record_No+=1
                            
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
                    PATH = "Files/" + str(TypeName) +"/" + str(TypeName) + str(index)

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

        if not Check_If_Type_Exits(TypeName, self.SystemCatalog):
            newType = Type(TypeName, 0, NumberOfFields, Fields_Names, [])
            self.SystemCatalog.Types.update({TypeName: newType})

            self.SystemCatalog.NumberOfTypes += 1
            PATH = "./indexFiles/" + str(TypeName) + "index"
            if not os.path.exists(PATH):
                os.mkdir("Files/" + str(TypeName))
            else:
                raise TypeAlreadyExists("Type's path does already exists")
            open(PATH, "a").close()
            maxNoofRecordsPerFile = int(2048 / (4 * NumberOfFields)) * 255
            newIndexFile = indexFile(0, maxNoofRecordsPerFile, [])
            self.SystemCatalog.indexFiles.update({TypeName: newIndexFile})
            print("The operation of creating type is compleated.")
        else:
            print(" The Type does already exists so we cannot add this one")

    def Delete_Type(self, TypeName):
        if Check_If_Type_Exits(TypeName, self.SystemCatalog):
            print("Deleting Type -", TypeName)
            print(str(TypeName) + "index")
            del self.SystemCatalog.Types[TypeName]
            self.SystemCatalog.NumberOfTypes -= 1
            del self.SystemCatalog.indexFiles[TypeName]
            PATHindexFile="indexFiles/" + str(TypeName) + "index"
            PATHtype="Files/" + str(TypeName)
            if os.path.exists(PATHtype):
                os.system("rm ./indexFiles/" + str(TypeName) + "index")
                os.system("rm -rf ./Files/" + str(TypeName))
                print("The operation of deleting type is compleated.")
            else:
                raise TypeDoesNotExists("Type'path does not exist so we cannot delete")
        else:
            print(" The Type does not exists so we cannot delete")


    def List_All_Types(self):
        try:
            sorted_list_of_All_Types = sorted(self.SystemCatalog.Types)
            for Type in sorted_list_of_All_Types:
                print(Type)
        except:
            raise TypeDoesNotExists("Type does not exist so we cannot list records")



class DML:
    def __init__(self, SystemCatalog):
        self.SystemCatalog = SystemCatalog

    def Create_Record(self, TypeName, Fields_Values):
        Type = self.SystemCatalog.Types[TypeName]
        if Type.NumberOfFields!=len(Fields_Values):
            raise FieldsAreNotValid("The number of Fields of this record does not matches with the its type ")
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
            flag_page=False
            while indexP >= 0:
                if flag_page==False:
                    NumberOfRecordOfPage = len(
                        f.Types[TypeName].Files[indexF].Pages[indexP].Records)
                    NumberOfRecordOfFile = NumberOfRecordOfFile+NumberOfRecordOfPage
                    if NumberOfRecordOfPage == 0:
                        del f.Types[TypeName].Files[indexF].Pages[indexP]
                    else:
                        flag_page=True
                        f.Types[TypeName].Files[indexF].Pages[indexP].NumberOfRecords = NumberOfRecordOfPage
                elif flag_page==True:
                    NumberOfRecordOfFile+=MaxNumberOfRecordsPerPage
                    f.Types[TypeName].Files[indexF].Pages[indexP].NumberOfRecords = MaxNumberOfRecordsPerPage
                indexP -= 1

            if NumberOfRecordOfFile == 0:
                if f.Types[TypeName].Files[indexF].PreviousFile!=255:
                    f.Types[TypeName].Files[f.Types[TypeName].Files[indexF].PreviousFile].NextFile=255
                del f.Types[TypeName].Files[indexF]
                PATH="Files/"+str(TypeName)+"/"+str(TypeName)+str(indexF)
                if  os.path.exists(PATH):
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

    def Search_Record(self, TypeName, PrimaryKey):
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
        for i in range(len(Fields)):
            print(i, "th Field is : ", Fields[i], " ", end="")

    def List_Records(self, TypeName):

        for File in self.SystemCatalog.Types[TypeName].Files:
            for Page in File.Pages:
                for Record in Page.Records:
                    for Field in Record.Fields:
                        print("th Field is : ", Field, " ", end="")


def Check_If_Type_Exits(TypeName, SystemCatalog):
    print("Checking If Type Exits...")
    for Type in SystemCatalog.Types:
        if Type == TypeName:
            print("The Type is found!!! ")
            return True
    return False
    print("The Type is available to create!!!")

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


if not os.path.exists("indexFiles"):
    os.mkdir("indexFiles")

if not os.path.exists("Files"):
    os.mkdir("Files")
with SystemCatalog() as f:

    d1 = DLL(f)

    d2 = DML(f)

    #d1.Create_Type("5", 10, ["age", "len", "spe","len","len","len","len","len","len","len"])  # 16
    d1.Create_Type("8", 4, ["age", "len", "spe", "smell"])  # 18
    d1.Create_Type("7", 3, ["age", "len", "spe"])  # 16
    d1.Create_Type("8", 4, ["agex", "lenx", "spex", "prox"])  # 16
    d1.Create_Type("1", 4, ["agex", "lenx", "spex", "prox"])  # 16

    d1.Delete_Type("5")

    d1.List_All_Types()

    """limit = 45000
    index = 17000
    lists = []
    lists2 = []
    for i in range(limit):
        lists.append(i)

    while index > 0:
        PK = lists.pop(randrange(len(lists)))
        lists2.append(PK)
        d2.Create_Record("5", [PK, 172, 45,1,2,3,4,5,6,7])
        index -= 1

    
    index = 13500

    
    while index > 0:
        PK = lists2.pop(randrange(len(lists2)))
             
        d2.Delete_Record("5", PK)
        index -= 1



    for Type in f.Types:
        print("We are examine Type: ",Type)
        print(" We have ",f.Types[Type].NumberOfFiles," number of Files")
        file_no=0
        for File in f.Types[Type].Files:
            print("In File ",file_no, "we have ",File.NumberOfRecords," NumberOfRecords")
            print("In File ",file_no, "we have ",File.NumberOfPages," NumberOfPages")
            print("In File ",file_no, "our next file is",File.NextFile)
            print("In File ",file_no, "our previous file is",File.PreviousFile)
            print()
            print("Now we are examine File",file_no,"' Pages")
            page_no=0
            for page in File.Pages:
                print("Page :",page_no," we have ",page.NumberOfRecords,"NumberOfRecords")
                page_no+=1
            file_no+=1

"""