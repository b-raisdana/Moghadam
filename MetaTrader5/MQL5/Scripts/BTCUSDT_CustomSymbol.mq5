// Define BTC/USDT custom symbol
input string SymbolName = "BTCUSDT"; // Symbol name
input double LotSize = 0.01; // Lot size
input double ContractSize = 1.0; // Contract size
input double InitialMargin = 100.0; // Initial margin requirement
input double MaintenanceMargin = 80.0; // Maintenance margin requirement
input datetime TradingStartTime = D'2023.01.01 00:00'; // Trading start time
input datetime TradingEndTime = D'2023.12.31 23:59'; // Trading end time

int OnInit()
{
   // Create the custom symbol
   int result = SymbolCreate(SymbolName, SYMBOL_FUTURES);
   if (result != SYMBOL_CREATE_OK)
   {
      Print("Error creating custom symbol: ", result);
      return(INIT_FAILED);
   }

   // Set trading session times
   SymbolSessionSet(SymbolName, SESSION_DEALS, true);
   SymbolSessionSet(SymbolName, SESSION_BUY_ORDERS, true);
   SymbolSessionSet(SymbolName, SESSION_SELL_ORDERS, true);

   // Configure symbol properties
   SymbolSetDouble(SymbolName, SYMBOL_VOLUME_MIN, LotSize);
   SymbolSetDouble(SymbolName, SYMBOL_VOLUME_MAX, LotSize);
   SymbolSetDouble(SymbolName, SYMBOL_VOLUME_STEP, LotSize);
   SymbolSetDouble(SymbolName, SYMBOL_VOLUME_LIMIT, LotSize);
   SymbolSetDouble(SymbolName, SYMBOL_MARGIN_INITIAL, InitialMargin);
   SymbolSetDouble(SymbolName, SYMBOL_MARGIN_MAINTENANCE, MaintenanceMargin);

   return(INIT_SUCCEEDED);
}

void OnDeinit(const int reason)
{
   // Delete the custom symbol when the script is removed from the chart
   SymbolDelete(SymbolName);
}
