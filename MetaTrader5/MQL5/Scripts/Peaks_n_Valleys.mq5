//+------------------------------------------------------------------+
//|                                              Peaks_n_Valleys.mq5 |
//|                                                          Behrooz |
//|                                             https://www.mql5.com |
//+------------------------------------------------------------------+
#property copyright "Behrooz"
#property link      "https://www.mql5.com"
#property version   "1.00"

#include <FileCSV.mqh>
#include <Arrays\ArrayString.mqh>
#include <Timeframe.mqh>

input bool show_1min = true;
input bool show_5min = true;
input bool show_15min = true;
input bool show_1H = true;
input bool show_4H = true;
input bool show_1D = true;
input bool show_1W = true;

//+------------------------------------------------------------------+
//|                                                                  |
//+------------------------------------------------------------------+
bool ShouldDisplayTimeframe(string timeframe)
  {
   if(timeframe == "1min")
      return show_1min;
   if(timeframe == "5min")
      return show_5min;
   if(timeframe == "15min")
      return show_15min;
   if(timeframe == "1H")
      return show_1H;
   if(timeframe == "4H")
      return show_4H;
   if(timeframe == "1D")
      return show_1D;
   if(timeframe == "1W")
      return show_1W;
   Print("Unsupported timeframe for ShouldDisplayTimeframe:" + timeframe);
   return false;
  }
//+------------------------------------------------------------------+
//| Expert initialization function                                   |
//+------------------------------------------------------------------+
//void update_object_display()
//  {
//   Print("update_object_display started");
//   int chart_period = ChartPeriod();
//   Print("Chart period:" + IntegerToString(chart_period));
//   int totalObjects = ObjectsTotal(0);
//   int hidden_object_count = 0;
//   int visible_object_count = 0;
//
//   for(int i = 0; i < totalObjects; i++)
//     {
//      string object_name = ObjectName(0, i);
//      if(chart_period <= TimeframeToPeriod(timeframe) && ShouldDisplayTimeframe(timeframe))
//        {
//         Print(chart_period+" <= "+timeframe+"/"+TimeframeToPeriod(timeframe));
//         ObjectSetInteger(0, object_name, OBJPROP_HIDDEN, false);
//         visible_object_count++;
//        }
//      else
//        {
//         Print(chart_period+" > "+timeframe+"/"+TimeframeToPeriod(timeframe));
//         ObjectSetInteger(0, object_name, OBJPROP_HIDDEN, true);
//         hidden_object_count++;
//        }
//     }
//   Print(IntegerToString(visible_object_count) + " Visible and " +
//         IntegerToString(hidden_object_count) + " Hidden objects updated.");
//  }
//+------------------------------------------------------------------+
//|                                                                  |
//+------------------------------------------------------------------+
int load_tops()
  {
   Print("load_tops started");
   CFileCSV fileCSV;
   string file_name = "multi_timeframe_peaks_n_valleys.csv";

   datetime start_time = NULL;
   datetime end_time = NULL;
   int counter = 0;

   if(fileCSV.Open(file_name, FILE_READ|FILE_CSV|FILE_ANSI))
     {
      string line;
      fileCSV.Read(line); // Read the header
      if(line != "timeframe,date,open,high,low,close,volume,peak_or_valley,strength")
        {
         Print("Invalid input file format in "+ file_name +line);
         return -1;
        }
      while(fileCSV.Read(line))
        {
         string parts[];
         StringSplit(line, ',', parts);
         //Print(line);
         string timeframe = parts[0];
         datetime time = StringToTime(parts[1]);
         double high = StringToDouble(parts[3]);
         double low = StringToDouble(parts[4]);
         double strength = StringToDouble(parts[8]);
         string peak_or_valley = parts[7];
         int top_period = int(StringToInteger(parts[8])/60);

         if(start_time == NULL  || start_time > time)
            start_time = time;
         if(end_time == NULL  || end_time <  time)
            end_time = time;

         color timeframe_color = TimeFrameColor(timeframe);
         string object_name;
         if(peak_or_valley == "valley")
           {
            object_name = "V " + timeframe + "@" + DateTimeToString(time);
            ObjectCreate(0, object_name, OBJ_ARROW_UP, 0, time, low);
            ObjectSetInteger(0, object_name, OBJPROP_COLOR, timeframe_color);
            string tooltip_text = StringFormat(object_name + "\nStrength: %s\nLow: %.2f", IntegerToString(strength), low);
            ObjectSetString(0, object_name, OBJPROP_TOOLTIP, tooltip_text);
            
           }
         else
            if(peak_or_valley == "peak")
              {
               object_name = "P " + timeframe + "@" + DateTimeToString(time);
               ObjectCreate(0, object_name, OBJ_ARROW_DOWN, 0, time, high+7);
               ObjectSetInteger(0, object_name, OBJPROP_COLOR, timeframe_color);
               string tooltip_text = StringFormat(object_name + "\nStrength: %s\nLow: %.2f", IntegerToString(strength), high);
               ObjectSetString(0, object_name, OBJPROP_TOOLTIP, tooltip_text);
              }
         SetObjectTimeframes(object_name);
         //if(ShouldDisplayTimeframe(timeframe))
         //   ObjectSetInteger(0, object_name, OBJPROP_HIDDEN, true);
         //else
         //   ObjectSetInteger(0, object_name, OBJPROP_HIDDEN, false);

         counter++;

        }
      fileCSV.Close();
      tops_are_loaded = true;
      Print(IntegerToString(counter) + " Tops added: " + TimeToString(start_time) +"-"+TimeToString(end_time));
      //int result = FileMove(file_name, 0 ,file_name + ".bak", FILE_REWRITE);
      //if (result == false) Print("Fail to rename "+file_name+" to .bak");
     }
   else
     {
      Print("Failed to open file. Error code: ", GetLastError());
     }
   return 1;
  }
//+------------------------------------------------------------------+
//|                                                                  |
//+------------------------------------------------------------------+
bool tops_are_loaded = false;
int OnInit()
  {
   Print("Peaks_n_Valleys OnInit");
   if(!tops_are_loaded)
      load_tops();
   //update_object_display();
   return(0);
  }

//+------------------------------------------------------------------+

//+------------------------------------------------------------------+
