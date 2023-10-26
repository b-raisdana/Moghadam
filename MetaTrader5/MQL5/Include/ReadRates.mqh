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
#include <CustomSymbols.mqh>

//+------------------------------------------------------------------+
//|                                                                  |
//+------------------------------------------------------------------+
int RatesNumber(string symbol)
  {
   MqlRates mql_rates[];
   int rates_count = CopyRates(Symbol(), PERIOD_M1, 1, 99999999, mql_rates);
   return rates_count;
  }
//+------------------------------------------------------------------+
//|                                                                  |
//+------------------------------------------------------------------+
bool ReadSymbolRates(string chart_symbol)
  {
// Setup custom symbol
   if(chart_symbol=="" || chart_symbol==NULL)
     {
      long chart_id = ChartID();
      chart_symbol = ChartSymbol(chart_id);
     }
// Open the CSV file
   CFileCSV csv;
   string file_name = chart_symbol+".ohlcv.csv";
//Print("Start reading "+file_name);
   if(!csv.Open(file_name, FILE_READ|FILE_CSV|FILE_ANSI))
     {
      Print("Failed to open "+file_name+" file. Error code: ", GetLastError());
      return false;
     }
// Read and ignore the first line (header line)
   string line;
   csv.Read(line);
   string expected_header = "date,open,high,low,close,volume";
   if(line != expected_header)
     {
      Print("Invalid input file format in "+ file_name + " " +line);
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
      if(CustomRatesUpdate(chart_symbol, tick)<0)
        {
         int error_code = GetLastError();
         Print("Failed to add tick. Error code: ", error_code);
         if(error_code==0)
           {
            MqlRates mql_rates[];
            Print("Number of rates:" + RatesNumber(chart_symbol));
            return false;
           }
        }
      read_lines += 1;
      if(MathMod(read_lines, 1000) == 0)
        {
         Print(IntegerToString(read_lines) + " lines of data imported from " + file_name + " Rates: " + RatesNumber(chart_symbol));
        }
     }
   Print(IntegerToString(read_lines) + " lines of data imported successfully");
   csv.Close();
   int result = FileMove(file_name, 0,file_name + ".bak", FILE_REWRITE);
   if(result == false)
      Print("Fail to rename "+file_name+" to .bak");

   return true;
  }
//+------------------------------------------------------------------+
void ReadCustomSymbolsRates()
  {
   for(int i = 0; i<ArraySize(custom_symbols); i++)
     {
      if(ReadSymbolRates(custom_symbols[i]))
         //Print(custom_symbols[i] + "ReadRates Succeed!");
         int nop = 0;
      else
         //Print(custom_symbols[i] + "ReadRates Failed!");
         int nop = 0;
     }
  }
//+------------------------------------------------------------------+
