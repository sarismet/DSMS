from random import randrange
import os


def insertRecord(arr, l, r, x):
    print(l, r)
    if r == l:
        if arr[r] > x:
            arr.insert(r, x)
            return

        else:
            arr.insert(r+1, x)
            return
    elif (r-l) == 1:
        if arr[l] < x and arr[r] > x:
            arr.insert(l, x)
            return
        elif arr[l] > x:
            return insertRecord(arr, l, l, x)
        elif arr[r] < x:
            return insertRecord(arr, r, r, x)
    elif r >= l:

        mid = l + (r - l)//2

        if arr[mid-1] < x and arr[mid] > x:
            arr.insert(mid, x)
            return

        elif arr[mid-1] > x:
            return insertRecord(arr, l, mid-1, x)

        # Else the element can only be present in right subarray
        elif arr[mid] < x:
            return insertRecord(arr, mid, r, x)


Records = [1]


for i in range(2, 10):
    insertRecord(Records, 0, len(Records)-1, i)


print("asd", Records)
