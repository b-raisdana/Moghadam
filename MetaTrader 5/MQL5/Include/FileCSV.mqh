//+------------------------------------------------------------------+
//|                                                          FileCSV.mqh |
//|                        Copyright 2023, MetaQuotes Software Corp. |
//|                                             https://www.mql5.com |
//+------------------------------------------------------------------+

class CFileCSV
{
private:
   int file_handle;

public:
   // Constructor
   CFileCSV()
   {
      file_handle = INVALID_HANDLE;
   }

   // Method to open a file
   bool Open(string file_name, int mode)
   {
      file_handle = FileOpen(file_name, mode);
      return file_handle != INVALID_HANDLE;
   }

   // Method to read a line from a file
   bool Read(string& line)
   {
      if(file_handle != INVALID_HANDLE)
      {
         line = FileReadString(file_handle);
         return StringLen(line) > 0;
      }
      return false;
   }


   // Method to close a file
   void Close()
   {
      if(file_handle != INVALID_HANDLE)
      {
         FileClose(file_handle);
         file_handle = INVALID_HANDLE;
      }
   }
};
//+------------------------------------------------------------------+
