//+------------------------------------------------------------------+
//|                                                    Timeframe.mqh |
//|                                                          Behrooz |
//|                                             https://www.mql5.com |
//+------------------------------------------------------------------+
#property copyright "Behrooz"
#property link      "https://www.mql5.com"
//+------------------------------------------------------------------+
//|                                                                  |
//+------------------------------------------------------------------+
struct ChartInfo
  {
   float             period;
   int               chart_object_period;
  };
//+------------------------------------------------------------------+
//|                                                                  |
//+------------------------------------------------------------------+
class Charts
  {
protected:
public:
   static ChartInfo  chart_periods[];
   static void       listKeys(float &keys[])
     {
      ArrayFree(keys);
      ArrayResize(keys, ArraySize(chart_periods));
      for(int i=0; i < ArraySize(chart_periods); i++)
        {
         keys[i] = chart_periods[i].period;
        }
     }
   static int        byKey(float key)
     {
      for(int i=0; i < ArraySize(chart_periods); i++)
        {
         if(chart_periods[i].period==key)
            return chart_periods[i].chart_object_period;
        }
      Print(key + " key not found in chart_periods!");
      return NULL;
     }
  };
static ChartInfo Charts::chart_periods[] =
  {
     {1.0, OBJ_PERIOD_M1},
     {5.0, OBJ_PERIOD_M5},
     {15.0, OBJ_PERIOD_M15},
     {30.0, OBJ_PERIOD_M30},
     {60.0, OBJ_PERIOD_H1},
     {240.0, OBJ_PERIOD_H4},
     {1440.0, OBJ_PERIOD_D1},
     {10080.0, OBJ_PERIOD_W1},
     {43200.0, OBJ_PERIOD_MN1},
  };
//int supportedPeriods[] =
//  {
//   PERIOD_M1,  PERIOD_M2,   PERIOD_M3,   PERIOD_M4,   PERIOD_M5,   PERIOD_M6,   PERIOD_M10,   PERIOD_M12,   PERIOD_M15,
//   PERIOD_M20,   PERIOD_M30,   PERIOD_H1,   PERIOD_H2,   PERIOD_H3,   PERIOD_H4,   PERIOD_H6,   PERIOD_H8,   PERIOD_H12,
//   PERIOD_D1,   PERIOD_W1,   PERIOD_MN1  };
//+------------------------------------------------------------------+
//|                                                                  |
//+------------------------------------------------------------------+
int               TimeframeToPeriod(string freq)
  {
   if(StringFind(freq, "min") >= 0)
      return int(StringToInteger(StringSubstr(freq, 0, StringFind(freq, "min"))));
   if(StringFind(freq, "H") >= 0)
      return int(StringToInteger(StringSubstr(freq, 0, StringFind(freq, "H"))))* 60;
   if(StringFind(freq, "D") >= 0)
      return PERIOD_D1; // Daily timeframe
   if(StringFind(freq, "W") >= 0)
      return PERIOD_W1; // Weekly timeframe
   if(StringFind(freq, "M") >= 0)
      return PERIOD_MN1; // Monthly timeframe
   return -1; // Invalid frequency string
  }
//+------------------------------------------------------------------+
//|                                                                  |
//+------------------------------------------------------------------+
int               PeriodToIndex(int period)
  {
   if(period <= 0)
      return -1; // Return -1 if the input is non-positive to prevent log(0) or log(negative number)

   double result = MathLog(period)/MathLog(2)/2.0;
   return (int)MathRound(result);
  }
//+------------------------------------------------------------------+
//|                                                                  |
//+------------------------------------------------------------------+
int               TimeframeToIndex(string timeframe)
  {
   int period = TimeframeToPeriod(timeframe);
   return PeriodToIndex(period);
  }

color timeframe_color_map[] =
  {
   clrRed,              // 1min
   clrOrange,           // 5min
   clrYellow,           // 15min
   clrLemonChiffon,     // 1H
   clrGreen,            // 4H
   clrCyan,             // 1D
   C'1F,45,6E',             // 4D
   clrBlue,             // 1W
  };
//+------------------------------------------------------------------+
//|                                                                  |
//+------------------------------------------------------------------+
color             TimeFrameColor(string timeframe)
  {
   int timeframe_index = TimeframeToIndex(timeframe);
   int number_of_colors = ArraySize(timeframe_color_map);
   int color_index = int(MathMod(timeframe_index, number_of_colors));
   return timeframe_color_map[color_index];
  }
//+------------------------------------------------------------------+
//|                                                                  |
//+------------------------------------------------------------------+
double            DurationToMinutes(string duration)
  {
   int daysPos = StringFind(duration, " days ");
   int hoursPos = StringFind(duration, ":", daysPos+6);
   int minutesPos = StringFind(duration, ":", hoursPos+1);

   string daysStr = StringSubstr(duration, 0, daysPos);
   string hoursStr = StringSubstr(duration, daysPos+6, hoursPos-daysPos-6);
   string minutesStr = StringSubstr(duration, hoursPos+1, minutesPos-hoursPos-1);

   int days = StringToInteger(daysStr);
   int hours = StringToInteger(hoursStr);
   int minutes = StringToInteger(minutesStr);

   double totalMinutes = (days * 24 * 60) + (hours * 60) + minutes;

   return totalMinutes;
  }

//+------------------------------------------------------------------+
//|                                                                  |
//+------------------------------------------------------------------+
string            DateTimeToString(datetime time)
  {
   MqlDateTime struct_time;
   TimeToStruct(time, struct_time);

   return StringFormat("%2d/%2d %2d:%2d:%2d",
                       struct_time.mon,
                       struct_time.day,
                       struct_time.hour,
                       struct_time.min,
                       struct_time.sec);
  }

//+------------------------------------------------------------------+
//|                                                                  |
//+------------------------------------------------------------------+
string            ObjectTimeframe(string object_name)
  {
   int start_pos = StringFind(object_name, " ") + 1;
   int end_pos = StringFind(object_name, "@") - start_pos;
   string timeframe = StringSubstr(object_name, start_pos, end_pos);
   return timeframe;
  }
//+------------------------------------------------------------------+
//|                                                                  |
//+------------------------------------------------------------------+
int               CombineGeTimeframes(string timeframe)
  {
   int result = 0;
   float supported_chart_periods[] = {};
   Charts::listKeys(supported_chart_periods);
   int timeframe_period = TimeframeToPeriod(timeframe);
   if(timeframe == "15min")
     {
      int a = 0;
     }
   for(int i = 0; i<ArraySize(supported_chart_periods); i++)
     {
      double chart_period_key = supported_chart_periods[i];
      int chart_period_value = NULL;
      chart_period_value = Charts::byKey(chart_period_key);
      if(timeframe_period >= chart_period_key)
         result = result | chart_period_value;
      else
         return result;
     }
   return result;
  }
//+------------------------------------------------------------------+
//|                                                                  |
//+------------------------------------------------------------------+
void              SetObjectTimeframes(string object_name)
  {
   string timeframe = ObjectTimeframe(object_name);
   int timeframes_to_display = CombineGeTimeframes(timeframe);
   Print(object_name+" timeframes_to_display:"+timeframes_to_display);
   if(!ObjectSetInteger(0, object_name, OBJPROP_TIMEFRAMES, timeframes_to_display))
     {
      Print("Failed to set object timeframes. Error code: ", GetLastError());
     }
  }
//+------------------------------------------------------------------+

//+------------------------------------------------------------------+
