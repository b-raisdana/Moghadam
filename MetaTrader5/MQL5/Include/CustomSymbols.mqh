//+------------------------------------------------------------------+
//|                                                CustomSymbols.mqh |
//|                                  Copyright 2023, MetaQuotes Ltd. |
//|                                             https://www.mql5.com |
//+------------------------------------------------------------------+
#property copyright "Copyright 2023, MetaQuotes Ltd."
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
string custom_symbols[] =
  {
   "CustomBTCUSDT",
   "CustomETHUSDT",
  };
 
 
bool CreateCustomSymbols()
  {
// Setup custom symbol
   for(int i=0; i<ArraySize(custom_symbols); i++)
     {
      string symbol_name = custom_symbols[i];
      bool is_symbol_custom;
      if(SymbolExist(symbol_name, is_symbol_custom))
        {
         continue;
        }
      Print(symbol_name + " does not exist...");
      if(!CustomSymbolCreate(symbol_name))
        {
         Print("Failed to create custom symbol "+symbol_name +". Error code: ", GetLastError());
         return false;
        }
      else
        {
         Print(symbol_name + " created");
         if(!SymbolSelect(symbol_name, true))
           {
            Print("Failed to add "+ symbol_name +" to Watch List. Error code: ", GetLastError());
           }
        }
     }
     return true;
  }
//+------------------------------------------------------------------+
