//+------------------------------------------------------------------+
//|                                                    ReadRates.mqh |
//|                                                          Behrooz |
//|                                             https://www.mql5.com |
//+------------------------------------------------------------------+
#property copyright "Behrooz"
#property link      "https://www.mql5.com"
//+------------------------------------------------------------------+
//| defines                                                          |
//+------------------------------------------------------------------+
// #define MacrosHello   "Hello, world!"
// #define MacrosYear    2010
//+------------------------------------------------------------------+
//| DLL imports                                                      |
//+------------------------------------------------------------------+
// #import "user32.dll"
//   int      SendMessageA(int hWnd,int Msg,int wParam,int lParam);
// #import "my_expert.dll"
//   int      ExpertRecalculate(int wParam,int lParam);
// #import
//+------------------------------------------------------------------+
//| EX5 imports                                                      |
//+------------------------------------------------------------------+
// #import "stdlib.ex5"
//   string ErrorDescription(int error_code);
// #import
//+------------------------------------------------------------------+
#include <FileCSV.mqh>
bool ReadRates(){
   // Setup custom symbol
   long chart_id = ChartID();
   string chart_symbol = ChartSymbol(chart_id);
   Print("Start reading "+chart_symbol+".csv");
   // Open the CSV file
   CFileCSV csv;
   string file_name = chart_symbol+".csv";
   if(!csv.Open(file_name, FILE_READ|FILE_CSV|FILE_ANSI))
   {
      Print("Failed to open "+file_name+" file. Error code: ", GetLastError());
      return false;
   }
   // Read and ignore the first line (header line)
   string line;
   csv.Read(line);   
   if (line != "date,open,high,low,close,volume"){
      Print("Invalid input file format in "+ file_name +line);
      return false;
   }
   int read_lines = 0;
   while(csv.Read(line))
   {
      string parts[];
      StringSplit(line, ',', parts);
      datetime time = StringToTime(parts[0]);
      double open = StringToDouble(parts[1]);
      double high = StringToDouble(parts[2]);
      double low = StringToDouble(parts[3]);
      double close = StringToDouble(parts[4]);
      double volume = StringToDouble(parts[5]);
      MqlRates tick[1];
      tick[0].time = time;
      tick[0].open = open;
      tick[0].high = high;
      tick[0].low = low;
      tick[0].close = close;
      tick[0].real_volume = int(volume * 1000000); // to preserve volume fractions as MQL does not allow volume fractions
      if(CustomRatesUpdate(chart_symbol, tick)<0){
         Print("Failed to add tick. Error code: ", GetLastError());
         return false;
      }
      read_lines += 1;
      if (MathMod(read_lines, 1000) == 0)    Print(IntegerToString(read_lines) + " lines of data imported successfully"); 
   }
   Print(IntegerToString(read_lines) + " lines of data imported successfully");
   csv.Close();
   int result = FileMove(file_name, 0 ,file_name + ".bak", FILE_REWRITE);
   if (result == false) Print("Fail to rename "+file_name+" to .bak");

   return true;
}