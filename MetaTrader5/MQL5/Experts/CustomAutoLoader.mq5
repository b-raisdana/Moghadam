//+------------------------------------------------------------------+
//|                                             CustomAutoLoader.mq5 |
//|                                                          Behrooz |
//|                                             https://www.mql5.com |
//+------------------------------------------------------------------+
#property copyright "Behrooz"
#property link      "https://www.mql5.com"
#property version   "1.00"
#include <ReadRates.mqh>
#include <CustomSymbols.mqh>
//+------------------------------------------------------------------+
//| Expert initialization function                                   |
//+------------------------------------------------------------------+
void ReadIndicatorUpdates()
  {

  }
//+------------------------------------------------------------------+
//|                                                                  |
//+------------------------------------------------------------------+
void IntializationUpdates()
  {
   if(CreateCustomSymbols())
      Print("create_custom_symbols Succeed!");
   else
      Print("create_custom_symbols Failed!");
  }
//+------------------------------------------------------------------+
//|                                                                  |
//+------------------------------------------------------------------+
void FrequentUpdates()
  {
   EventSetTimer(5);
   ReadCustomSymbolsRates();
   ReadIndicatorUpdates();
  }
//+------------------------------------------------------------------+
//|                                                                  |
//+------------------------------------------------------------------+
int OnInit()
  {
//--- create timer
   Print("CustomAutoLoader OnInit");
   EventSetTimer(300);
   IntializationUpdates();
   OnTimer();
   return(INIT_SUCCEEDED);
  }
//+------------------------------------------------------------------+
//| Expert deinitialization function                                 |
//+------------------------------------------------------------------+
void OnDeinit(const int reason)
  {
//--- destroy timer
   EventKillTimer();

  }
//+------------------------------------------------------------------+
//| Expert tick function                                             |
//+------------------------------------------------------------------+
void OnTick()
  {
//---

  }
//+------------------------------------------------------------------+
//| Timer function                                                   |
//+------------------------------------------------------------------+
void OnTimer()
  {
   Comment("\r\n\r\nLast Updated:" + TimeToString(TimeCurrent(), TIME_MINUTES | TIME_SECONDS));
   Print("Updated:" + TimeToString(TimeCurrent(), TIME_MINUTES | TIME_SECONDS));
   FrequentUpdates();
  }
//+------------------------------------------------------------------+
//| Trade function                                                   |
//+------------------------------------------------------------------+
void OnTrade()
  {
//---

  }
//+------------------------------------------------------------------+
//| TradeTransaction function                                        |
//+------------------------------------------------------------------+
void OnTradeTransaction(const MqlTradeTransaction& trans,
                        const MqlTradeRequest& request,
                        const MqlTradeResult& result)
  {
//---

  }
//+------------------------------------------------------------------+
//| Tester function                                                  |
//+------------------------------------------------------------------+
double OnTester()
  {
//---
   double ret=0.0;
//---

//---
   return(ret);
  }
//+------------------------------------------------------------------+
//| TesterInit function                                              |
//+------------------------------------------------------------------+
void OnTesterInit()
  {
//---

  }
//+------------------------------------------------------------------+
//| TesterPass function                                              |
//+------------------------------------------------------------------+
void OnTesterPass()
  {
//---

  }
//+------------------------------------------------------------------+
//| TesterDeinit function                                            |
//+------------------------------------------------------------------+
void OnTesterDeinit()
  {
//---

  }
//+------------------------------------------------------------------+
//| ChartEvent function                                              |
//+------------------------------------------------------------------+
void OnChartEvent(const int id,
                  const long &lparam,
                  const double &dparam,
                  const string &sparam)
  {
//---

  }
//+------------------------------------------------------------------+
//| BookEvent function                                               |
//+------------------------------------------------------------------+
void OnBookEvent(const string &symbol)
  {
//---

  }
//+------------------------------------------------------------------+
