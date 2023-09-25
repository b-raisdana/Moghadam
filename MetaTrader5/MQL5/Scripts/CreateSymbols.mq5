//+------------------------------------------------------------------+
//|                                          CustomSymbolBTCUSDT.mq5 |
//|                                                          Behrooz |
//|                                             https://www.mql5.com |
//+------------------------------------------------------------------+
#property copyright "Behrooz"
#property link      "https://www.mql5.com"
#property version   "1.00"

#include <FileCSV.mqh>


void OnStart()
{
   // Setup custom symbol
   string custom_symbols[] = {
      "CustomBTCUSDT",
      "CustomETHUSDT",
   };
   for(int i=0; i<ArraySize(custom_symbols); i++){
      string symbol_name = custom_symbols[i];
      if(!CustomSymbolCreate(symbol_name))
      {
         Print("Failed to create custom symbol "+symbol_name +". Error code: ", GetLastError());
         return;
      }
      else{
         Print(symbol_name + " created");
         if(!SymbolSelect(symbol_name, true)){
            Print("Failed to add "+ symbol_name +" to Watch List. Error code: ", GetLastError());
         }
      }
   }
   
  
}
