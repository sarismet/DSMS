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
            # we first read the Number of types. unpack returns a tuple but the first element of this tuple is always what we want 
            self.SystemCatalogFile = open("SystemCatalog", "rb")
            self.NumberOfTypes = struct.unpack(
                "i", self.SystemCatalogFile.read(4))[0]
            print("There are ", self.NumberOfTypes, " Number Of Types.")
            # accoring to this number we create a loop and start creating type data structures
            for i in range(self.NumberOfTypes):
                # here we read the type's name
                Temp_Type_Name = self.SystemCatalogFile.read(
                    10).decode("utf-8")
                # if there is a white space we get rid of them
                Type_Name = re.sub(" ", "", Temp_Type_Name)
                # we read here the number of files
                File_No = struct.unpack("B", self.SystemCatalogFile.read(1))[0]
                # we read here the number of fields
                Fields_No = struct.unpack(
                    "B", self.SystemCatalogFile.read(1))[0]
                Fields = []

                # According to this number we create a loop and collect all fields names
                for b in range(Fields_No):
                    # read the field names and get rid of the whitespaces
                    tempx = self.SystemCatalogFile.read(10).decode("utf-8")
                    tempx = re.sub(" ", "", tempx)
                    Fields.append(tempx)
                Files = []
                print("Fields", Fields)
                # According to File_no we create a loop and start building file data structures
                for c in range(int(File_No)):

                    
                    # here we open the file using the index "c" and rb means read binary file
                    FileOpen = open(
                        "Files/" + str(Type_Name) + "/" +
                        str(Type_Name) + str(c), "rb"
                    )

                    # we read number of records of a file here.
                    NumberOfRecordsinFileHeader = struct.unpack("i", FileOpen.read(4))[
                        0
                    ]
                    # we read number of pages of a file here.
                    NumberOfPageinFileHeader = struct.unpack(
                        "B", FileOpen.read(1))[0]
                    # we read the ıd of next file 
                    NextFileinFileHeader = struct.unpack(
                        "B", FileOpen.read(1))[0]
                    # we read the ıd of previous file 
                    PreviousFileinFileHeader = struct.unpack(
                        "B", FileOpen.read(1))[0]

                    Pages = []
                    # According to this NumberOfPageinFileHeader we create a loop and read the all pages
                    for d in range(int(NumberOfPageinFileHeader)):

                        Records = []
                        # bif be means read this one byte as an unsigned number and read the number of records in page header
                        NumberOfRecordsinPageHeader = struct.unpack(
                            "B", FileOpen.read(1))[0]
                        # we create the bandwith by multipling one record size which is 4*how many fields it has and NumberOfRecordsinPageHeader
                        bandwidth = NumberOfRecordsinPageHeader*4*Fields_No
                        
                        # and read the page here and assing the string into AllPage.
                        AllPage = FileOpen.read(bandwidth)

                        indexN = 0

                        # and we read the AllPage field by field and create a record
                        Record_No = 0
                        while indexN < (NumberOfRecordsinPageHeader*4*Fields_No):
                            #we collect all the fields of a record here
                            Fields_Of_NewRecord = []
                            for f in range(Fields_No):
                                #a field is 4 byte and according to the number of fields we create a loop and read the fields.
                                Fields_Of_NewRecord.append(
                                    int(struct.unpack(
                                        "i", AllPage[indexN:indexN+4])[0]))
                                indexN = indexN+4
                            # we create the record object here.
                            Records.append(Record(Fields_Of_NewRecord))
                            
                            Record_No += 1
                        # and when we finish the while loop we reach out the end of the page and create its page object here
                        Pages.append(
                            Page(NumberOfRecordsinPageHeader, Records))

                    #when we finish for loop we create the file object here and append the files list
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
                
                # when we finish all reading operations we create type objects

                T = Type(Type_Name, File_No, Fields_No, Fields, Files)

                # and add the type into types directory
                self.Types.update({str(Type_Name): T})
            self.SystemCatalogFile.close()
            print("Reading All Type Files is Compleated")

            print("Phase 2 ....Reading All indexFiles.... ")
            # now we are ready to read indexFiles
            self.readindexFiles()
        except Exception as e:
            print(e)
            self.SystemCatalogFile.close()

    def readindexFiles(self):
        print("All indexFiles are being read...")
        for TypeName in self.Types:
            try:
                # we open the index files here
                filepath = "./indexFiles/" + str(TypeName) + "index"

                TindexFile = open(filepath, "rb")

                # read the number of records
                NumberOfRecords = struct.unpack("i", TindexFile.read(4))[0]
                # and read the number of records per file here
                NumberOfRecordsPerFile = struct.unpack(
                    "i", TindexFile.read(4))[0]

                Temp_Records_Array = []
                # according to number of records we create a loop and create index_record here and append Temp_Records_Array list
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
                # and when we finish we are ready to create indexFile object
                TI = indexFile(
                    NumberOfRecords, NumberOfRecordsPerFile, Temp_Records_Array
                )
                # finally we add the indexFile to indexFiles dictionary.
                self.indexFiles.update({TypeName: TI})
                TindexFile.close()
                print("Reading All  indexFiles is Compleated")
            except Exception as e:
                print(e)

    def writeback(self):

        print("writing back on System Catalog File...")
        try:
            self.SystemCatalogFile = open("SystemCatalog", "wb")
            # we write the number of types on SystemCatalog file
            self.SystemCatalogFile.write(struct.pack("i", self.NumberOfTypes))
            # we traverse all the types and write its all information. Main idea is what we read need to be written on its regarding files.
            for TypeName in self.Types:
                # we assume that the length of a typename can be max 10 character long then we need to add whitespaces until we have 10 character long.
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
                    # if we dont have physical we create its file.
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

    # we write all the information (FileID,PageID...) of records of indexfiles here.
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
    # this function is designed to complete the string 10 character long.
    def complate(self, TypeName):
        for i in range(len(TypeName), 10):
            TypeName = " " + TypeName
        return TypeName

# In this data structures we process the operation of DLL operations.
class DLL:
    # we need the system catalog we read above
    def __init__(self, SystemCatalog):
        self.SystemCatalog = SystemCatalog
    # here we create the type 
    def Create_Type(self, TypeName, NumberOfFields, Fields_Names):
        #first type is 0 number of files so we assign 0 here and empty pages list.
        # and we add thenew type its directory
        newType = Type(TypeName, 0, NumberOfFields, Fields_Names, [])
        self.SystemCatalog.Types.update({TypeName: newType})
        # increase the number of types
        self.SystemCatalog.NumberOfTypes += 1
        PATH = "./indexFiles/" + str(TypeName) + "index"
        # create the its directory here. 100% we don't have the directory but in any case we check.
        if not os.path.exists(PATH):
            os.mkdir("Files/" + str(TypeName))
        # after create the directory we create the file to store inside it.
        open(PATH, "a").close()
        # we can store max 255 record in a page so if we have 1 or 2 fields then we can have 512 and 256 records respectively so we need to have an restriction on it.
        maxNoofRecordsPerPage= int(2048 / (4 * NumberOfFields))
        if maxNoofRecordsPerPage>255:
            maxNoofRecordsPerPage=255
        # calculate the number of records per file by multiplying 255 since we can have max 255 page.
        maxNoofRecordsPerFile = maxNoofRecordsPerPage* 255
        newIndexFile = indexFile(0, maxNoofRecordsPerFile, [])
        self.SystemCatalog.indexFiles.update({TypeName: newIndexFile})
        print("The operation of creating type is compleated.")

    def Delete_Type(self, TypeName):

        print("Deleting Type -", TypeName)
        print(str(TypeName) + "index")
        # delete the type in type directory.
        del self.SystemCatalog.Types[TypeName]
        self.SystemCatalog.NumberOfTypes -= 1
        # delete the type indexfile
        del self.SystemCatalog.indexFiles[TypeName]
        #creating the paths of indexFile and Type
        PATHindexFile = "indexFiles/" + str(TypeName) + "index"
        PATHtype = "Files/" + str(TypeName)
        # we need to check if the directory is present since we can delete. If there is no type to delete we can crash.
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
    # here we traverse the types and print them after sorting.
    def List_All_Types(self, outputFile):

        sorted_list_of_All_Types = sorted(self.SystemCatalog.Types)
        for Type in sorted_list_of_All_Types:
            print(Type)
            outputFile.write(str(Type)+"\n")


# In this data structures we process the operation of DML operations.
class DML:
    # we need the system catalog we read above
    def __init__(self, SystemCatalog):
        self.SystemCatalog = SystemCatalog
    # we create the record there
    def Create_Record(self, TypeName, Fields_Values):
        # first we take the its regarding type from types dictionary and its indexFiles from indexFiles dictionary.
        Type = self.SystemCatalog.Types[TypeName]

        indexFile = self.SystemCatalog.indexFiles[TypeName]

        # Take the max number of records per file here.
        MaxNumberOfRecordsPerFile = indexFile.Max_Number_OF_Records_Per_File
        # and divide the value by 255 to have max number of records per page here.
        MaxNumberOfRecordsPerPage = int(MaxNumberOfRecordsPerFile / 255)

        # lets have a change and use the indexFile instead main_indexFile.
        main_indexFile = self.SystemCatalog.indexFiles[TypeName]

        # create the new record here
        newRecord = Record(Fields_Values)
        # if we are creating the first record then we create these values and we are done.
        if len(main_indexFile.Records) == 0:
            # create the its record_indexFile. FileID=0,PageID=0,RecordID=0,PrimaryKey=first field.
            newRecord = Record_indexFile(0, 0, 0, Fields_Values[0])
            main_indexFile.add_new_indexFile_Record(newRecord)
            # we are creating the first page of the type.
            newPage = Page(1, [Record(Fields_Values)])
            # and first file here.
            newFile = File(1, 1, 255, 255, [newPage])
            # and we are adding the first file its regarding type.
            self.SystemCatalog.Types[TypeName].addFile(newFile)
            return

        # if we are not creating the first record we need to first detect the place of new record in insert_Record_To_indexFile function.

        Returned_Index = insert_Record_To_indexFile(
            Fields_Values, main_indexFile, Type, MaxNumberOfRecordsPerPage
        )

        # and we need to update the places of all records in a type.
        index = Returned_Index
        # we need to traverse at the end of the record.
        while index < main_indexFile.Number_OF_Records:

            # we first increase the RecordID 
            main_indexFile.Records[index].RecordID += 1
            # if we exceed the limit 
            if main_indexFile.Records[index].RecordID == MaxNumberOfRecordsPerPage:
                # we need to update the record id 0 and increase the pageid
                main_indexFile.Records[index].RecordID = 0
                main_indexFile.Records[index].PageID += 1
                # if we reach the limit then 
                if main_indexFile.Records[index].PageID == 255:
                    # we need to update the page ıd by 0.
                    main_indexFile.Records[index].PageID = 0
                    # we need to increase the file id.
                    main_indexFile.Records[index].FileID += 1
                    # in this file statement we check if the file exists. if the if statment true then it means we don't have file, we need to create the file at first.
                    if main_indexFile.Records[index].FileID == len(Type.Files):
                        # linking the files
                        preFileID = main_indexFile.Records[index].FileID - 1
                        # creating the files.
                        newFile = File(1, 1, 255, preFileID, [])
                        Type.Files[main_indexFile.Records[index].FileID -
                                   1].NextFile = main_indexFile.Records[index].FileID
                        # we are creating the first page
                        newPage = Page(1, [])
                        # and pop the last record of previous file
                        newRecord = (
                            Type.Files[main_indexFile.Records[index].FileID - 1]
                            .Pages[254]
                            .Records.pop()
                        )
                        # and we append popped record into the new file 
                        newPage.Records.append(newRecord)
                        newFile.Pages.append(newPage)
                        Type.addFile(newFile)
                        # and we are done we return.
                        return
                    # if we have the file and the file is last file. The last is important to increase the number of page or record. 
                    # If we are not in last file then we dont change any variable.
                    if (
                        Type.Files[main_indexFile.Records[index].FileID].NumberOfRecords
                        < MaxNumberOfRecordsPerPage * 255
                    ):
                    # then we need to check first page is last page of a file
                        if (
                            Type.Files[main_indexFile.Records[index].FileID]
                            .Pages[0]
                            .NumberOfRecords
                            < MaxNumberOfRecordsPerPage
                        ):
                        # if it is then increase the number of records variable

                            Type.Files[main_indexFile.Records[index].FileID].Pages[
                                0
                            ].NumberOfRecords += 1
                        Type.Files[
                            main_indexFile.Records[index].FileID
                        ].NumberOfRecords += 1
                    # after all pop the last record of previous record and insert the first place of the current file.
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
                # if we don't have the page we need to first create the page to store the record.
                if (
                    Type.Files[main_indexFile.Records[index].FileID].NumberOfPages
                    == main_indexFile.Records[index].PageID
                ):
                    newPage = Page(0, [])
                    Type.Files[main_indexFile.Records[index].FileID].addPage(
                        newPage)
                # if the page we are considering pop the last record of previous page and insert the popped record in first place of the page.
                # if the cuurent page is the last page then we increase the number of records here.
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

    # we delete the record here by using TypeName and PrimaryKey 
    def Delete_Record(self, TypeName, PrimaryKey):

        # we first detect the place of record in its indexFile. FindRecord funtion returns us the indexFile record and its index
        Returned_Tupple = FindRecord(
            self.SystemCatalog.indexFiles[TypeName].Records,
            0,
            len(self.SystemCatalog.indexFiles[TypeName].Records) - 1,
            PrimaryKey,
        )
        # take the info of record
        Record = Returned_Tupple[0]
        FileID = Record.FileID
        PageID = Record.PageID
        RecordID = Record.RecordID
        # delete the record in its file by using info gathered above
        del (
            self.SystemCatalog.Types[TypeName]
            .Files[FileID]
            .Pages[PageID]
            .Records[RecordID]
        )
        # delete the record in its indexFile and decrease the number of records in indexFile by 1.
        self.SystemCatalog.indexFiles[TypeName].Number_OF_Records -= 1
        del self.SystemCatalog.indexFiles[TypeName].Records[Returned_Tupple[1]]
        index = Returned_Tupple[1]

        # calculate the number of records per file and per page here
        MaxNumberOfRecordsPerFile = self.SystemCatalog.indexFiles[
            TypeName].Max_Number_OF_Records_Per_File
        MaxNumberOfRecordsPerPage = int(MaxNumberOfRecordsPerFile / 255)
        main_indexFile = self.SystemCatalog.indexFiles[TypeName]

        # and we travese the all indexFile records list 
        while index < self.SystemCatalog.indexFiles[TypeName].Number_OF_Records:
            #decrease the recordid by 1
            main_indexFile.Records[index].RecordID -= 1
            # if it is -1 then we need to go previous page
            if main_indexFile.Records[index].RecordID == -1:
                # assign max the record id 
                main_indexFile.Records[index].RecordID = MaxNumberOfRecordsPerPage - 1
                # and decrease the pageid by 1.
                main_indexFile.Records[index].PageID -= 1
                # if the page id is -1 then we need to go previous file
                if main_indexFile.Records[index].PageID == -1:
                    # assign 254 to page id since max page id can be 254
                    main_indexFile.Records[index].PageID = 254
                    # and decrease the file id by 1
                    main_indexFile.Records[index].FileID -= 1
                    # pop the first page's first record of next file and 
                    Record_poped = (
                        self.SystemCatalog.Types[TypeName]
                        .Files[main_indexFile.Records[index].FileID + 1]
                        .Pages[0]
                        .Records.pop(0)
                    )
                    #append the popped record into the last page
                    self.SystemCatalog.Types[TypeName].Files[main_indexFile.Records[index].FileID].Pages[254].Records.append(
                        Record_poped)
                # if we dont change the file then apply the same confiquration but this time use current file
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
        # after the loop we need to update the variables of data structures.
        indexF = len(f.Types[TypeName].Files)-1
        # we starts with the last file
        while indexF >= 0:
            # take the number of pages of old version file 
            indexP = f.Types[TypeName].Files[indexF].NumberOfPages-1
            NumberOfRecordOfFile = 0
            # this bool variable is created to see if we have a record in a page. If so then we can asssign max number of records value to previous pages.
            # when we have a record in a page this bool type will be true.
            flag_page = False
            while indexP >= 0:
                # if it is false then see the size of records list in a page and assign the value to numberofrecords variable.
                if flag_page == False:
                    NumberOfRecordOfPage = len(
                        f.Types[TypeName].Files[indexF].Pages[indexP].Records)
                    NumberOfRecordOfFile = NumberOfRecordOfFile+NumberOfRecordOfPage
                    # if there is no record then delete the page 
                    if NumberOfRecordOfPage == 0:
                        del f.Types[TypeName].Files[indexF].Pages[indexP]
                    # else make the flag_page bool variable true
                    else:
                        flag_page = True
                        f.Types[TypeName].Files[indexF].Pages[indexP].NumberOfRecords = NumberOfRecordOfPage
                # if it is true then assign max number of record per page 
                elif flag_page == True:
                    NumberOfRecordOfFile += MaxNumberOfRecordsPerPage
                    f.Types[TypeName].Files[indexF].Pages[indexP].NumberOfRecords = MaxNumberOfRecordsPerPage
                indexP -= 1
            #if the file has no records then delete and if it is not the first file cut the link between previous file.
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
        # then update the number of files of the type.
        f.Types[TypeName].NumberOfFiles = len(f.Types[TypeName].Files)
    # here we are updating the record by using TypeName and new Fields parameters
    def Update_Record(self, TypeName, new_Fields_Values):
        # again we need to find the place of the record.
        Record = FindRecord(
            self.SystemCatalog.indexFiles[TypeName].Records,
            0,
            len(self.SystemCatalog.indexFiles[TypeName].Records) - 1,
            new_Fields_Values[0],
        )[0]
        # take the informations of record
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
        # go to the record in its file and use update fields function. This function just assign new fields array to previous fields array.
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
    # this is nearly the same as update record but we dont update the fields instead we write the fields on outputFile
    def Search_Record(self, TypeName, PrimaryKey, outputFile):
        # find the location of record
        Record = FindRecord(
            self.SystemCatalog.indexFiles[TypeName].Records,
            0,
            len(self.SystemCatalog.indexFiles[TypeName].Records) - 1,
            PrimaryKey,
        )[0]
        # get the infos of the record and
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
        # print out the fields
        for Field in Fields:
            print(i, "th Field is : ", Field, " ", end="")
            outputFile.write(str(Field)+" ")
            i += 1
        outputFile.write("\n")
    # we traverse the all files of type and print out the all fields
    def List_Records(self, TypeName, outputFile):

        for File in self.SystemCatalog.Types[TypeName].Files:
            for Page in File.Pages:
                for Record in Page.Records:
                    for Field in Record.Fields:
                        print("th Field is : ", Field, " ", end="")
                        outputFile.write(str(Field)+" ")
                    outputFile.write("\n")

# this function is exactly the same as binary search.
def FindRecord(Records, begin, end, PrimaryKey):

    # Check base case
    if end >= begin:

        mid = begin + (end - begin) // 2

        # If element is present at the middle itself
        if Records[mid].PrimaryKey == PrimaryKey:
            return (Records[mid], mid)

        # If element is smaller than mid, then it can only
        # be present in left subarray
        elif Records[mid].PrimaryKey > PrimaryKey:
            return FindRecord(Records, begin, mid - 1, PrimaryKey)

        # Else the element can only be present in right subarray
        else:
            return FindRecord(Records, mid + 1, end, PrimaryKey)

    else:
        # Element is not present in the array
        raise RecordisNotFound("Primary Key : ", PrimaryKey, " is not found")

# this function is nearly the same as binary search but if we have 1 or 2 size subarray then we nned to consider special cases.
def FindPlaceOfRecord(Records, begin, end, PrimaryKey):
    # if we have 1 size sub array then 
    if end == begin:
        # we check if the given PrimaryKey is less then the Records[end].PrimaryKey
        # if it is so then return the records index
        if Records[end].PrimaryKey > PrimaryKey:
           
            return end

        else:
            
            return end + 1
    # if we have 2 size sub array we need to check if we can insert the primary key between them if not divide the 2 sub array with size 1 and keep recursive function.
    elif (end - begin) == 1:
        if Records[begin].PrimaryKey < PrimaryKey and Records[end].PrimaryKey > PrimaryKey:
            
            return end
        elif Records[begin].PrimaryKey > PrimaryKey:
            return FindPlaceOfRecord(Records, begin, begin, PrimaryKey)
        elif Records[end].PrimaryKey < PrimaryKey:
            return FindPlaceOfRecord(Records, end, end, PrimaryKey)
    # The rest is the same as binary search
    elif end >= begin:

        mid = begin + (end - begin) // 2
        # we always keep checking if we can insert the primary key between two record
        if Records[mid - 1].PrimaryKey < PrimaryKey and Records[mid].PrimaryKey > PrimaryKey:
            
            return mid

        elif Records[mid - 1].PrimaryKey > PrimaryKey:
            return FindPlaceOfRecord(Records, begin, mid - 1, PrimaryKey)

        # Else the element can only be present in right subarray
        elif Records[mid].PrimaryKey < PrimaryKey:
            return FindPlaceOfRecord(Records, mid, end, PrimaryKey)

# here we insert the new created record in its indexFile and its normal file.
def insert_Record_To_indexFile(Fields, TheindexFile, Type, MaxNumberOfRecordsPerPage):
    TheindexFile.Number_OF_Records += 1

    index = 0
    PrimaryKey = Fields[0]
    # we first detect where we can insert the new record 
    index = FindPlaceOfRecord(
        TheindexFile.Records, 0, TheindexFile.Number_OF_Records - 2, PrimaryKey
    )
    # if the index is 0 then we need to insert the new record in first file's first page's first record place
    if index == 0:
        # after creating indexfile record and normal record insert it. 
        newRecord = Record_indexFile(0, 0, 0, PrimaryKey)
        TheindexFile.Records.insert(index, newRecord)
        Type.Files[0].Pages[0].Records.insert(0, Record(Fields))
        # the first file is not full then we can increase the number of record variable 
        if Type.Files[0].NumberOfRecords < 255 * MaxNumberOfRecordsPerPage:
            # the first page is not full then we can increase the number of record variable 
            if Type.Files[0].Pages[0].NumberOfRecords < MaxNumberOfRecordsPerPage:
                Type.Files[0].Pages[0].NumberOfRecords += 1
            Type.Files[0].NumberOfRecords += 1
        # we return 1 since we consider after the first record.
        return 1
    # if the index is not 0 then we take the information previous record and return the index the other operations are like above.
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

# Here we take the names of input and output files
inputFile = ""
outputFile = ""

if(len(sys.argv) > 1):
    inputFile = str(sys.argv[1])
    outputFile = str(sys.argv[2])
# if we are executing the program at the first time then create these folder to store files and indexFiles in it.
if not os.path.exists("indexFiles"):
    os.mkdir("indexFiles")

if not os.path.exists("Files"):
    os.mkdir("Files")
# here we create the SystemCatalog and assign it to f
with SystemCatalog() as f:
    # creating the DLL and DML
    d1 = DLL(f)

    d2 = DML(f)
    # reading the inputfile
    ReadingInputFile = open(inputFile, "r")
    # assign all the lines into Lines list.
    Lines = ReadingInputFile.readlines()
    #opening output file.
    WritingOnOutFile = open(outputFile, "w")

    for line in Lines:
        # divide the string by using white space delimeter
        lists = line.strip().split()
        # determine the command
        Command = lists[0]+" "+lists[1]
        if Command == "create type":
            print("The command is create type")
            try:
                d1.Create_Type(str(lists[2]), int(lists[3]), lists[4:])

            except:
                print("In valid input")

        elif Command == "delete type":
            print("The command is delete type")
            try:
                d1.Delete_Type(str(lists[2]))
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
                # when we read the input it is string but we need integer so we need to convert them to int.
                Fields = [int(i) for i in lists[3:]]
                d2.Create_Record(str(lists[2]), Fields)
            except:
                print("In valid input")
        elif Command == "delete record":
            print("The command is delete record")
            try:
                d2.Delete_Record(str(lists[2]), int(lists[3]))
            except:
                print("In valid input")
        elif Command == "update record":
            print("The command is update record")
            try:
                # when we read the input it is string but we need integer so we need to convert them to int.
                Fields = [int(i) for i in lists[3:]]
                d2.Update_Record(str(lists[2]), Fields)
            except:
                print("In valid input")
        elif Command == "search record":
            print("The command is search record")
            try:
                d2.Search_Record(str(lists[2]), int(lists[3]), WritingOnOutFile)
            except:
                print("In valid input")
        elif Command == "list record":
            print("The command is list record")
            try:
                d2.List_Records(str(lists[2]), WritingOnOutFile)
            except:
                print("In valid input")

    
    ReadingInputFile.close()
    WritingOnOutFile.close()
