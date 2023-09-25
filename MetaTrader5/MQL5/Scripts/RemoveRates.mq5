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
   long chart_id = ChartID();
   string chart_symbol = ChartSymbol(chart_id);
   if(StringFind(chart_symbol, "Custom") != 0){
      Print("This script designed spesifically for removing Custom symbols but "+ 
         chart_symbol+ " does not looking like Custom symbols");
      return;
   }
 
   int number_of_deleted_rates = CustomRatesDelete(
      chart_symbol,       // symbol name
      datetime("1970-01-01 00:00:00"),         // start date
      datetime("2024-01-01 00:00:00")           // end date
   );
   if (number_of_deleted_rates != -1){
      Print(number_of_deleted_rates + " rates deleted");
   }
   else{
      Print("CustomRatesDelete Error code: ", GetLastError());
   }
   ChartRedraw(chart_id);
   return;
   
   
   
   Print("Chart Symbol:"+chart_symbol);
   if (ChartClose(chart_id))
   {
      Print("Chart closed successfully");
   }
   else
   {
      Print("Failed to close chart. Error code: ", GetLastError());
   }
   if(SymbolSelect(chart_symbol, false)){
      Print(chart_symbol + " removed from Watch List");
   }
   else
   {
      Print("Unable to remove " + chart_symbol + " from Watch List");
      Print("SymbolSelect Error code: ", GetLastError());
   }
   if (!CustomSymbolDelete(chart_symbol)){
      Print("unable to delete:" + chart_symbol);
   }
   else{
      Print(chart_symbol+" deleted!");
   }
   return;
}
