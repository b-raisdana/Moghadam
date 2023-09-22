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
void OnStart()
{
   Print("RunRates OnStart");
   if(ReadRates()) Print("ReadRates Succeed!" ); else Print("ReadRates Failed!" );
}
