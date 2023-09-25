//+------------------------------------------------------------------+
//|                                          CustomSymbolBTCUSDT.mq5 |
//|                                                          Behrooz |
//|                                             https://www.mql5.com |
//+------------------------------------------------------------------+
#property copyright "Behrooz"
#property link      "https://www.mql5.com"
#property version   "1.00"


#import "kernel32.dll"
   int MoveFileA(string ExistingFilename, string NewFilename);
#import

#include <ReadRates.mqh>
#include <CustomSymbols.mqh>
void OnStart()
{
   Print("RunRates OnStart");
   for(int i = 0; i<ArraySize(custom_symbols); i++){
      if(ReadRates(custom_symbols[i])) Print(custom_symbols[i] + "ReadRates Succeed!" ); else Print(custom_symbols[i] + "ReadRates Failed!" );
   }
   
}
