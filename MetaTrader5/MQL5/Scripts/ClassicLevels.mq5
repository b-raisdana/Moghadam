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
// Function to draw a colored box
int DrawColoredBox(datetime start, datetime end, double top, double bottom, color fill_color, string object_name, color border_color)
  {
   int rectangle_handle = ObjectCreate(0, object_name, OBJ_RECTANGLE, 0, start_time, start_price, end_time, end_price);
   ObjectSetInteger(0, object_name, OBJPROP_COLOR_BG, fill_color | 0x10000000); // Use the high bit for transparency
   ObjectSetInteger(0, object_name, OBJPROP_COLOR, clrNone);
   if(border_color == NULL)
      ObjectSetInteger(0, object_name, OBJPROP_WIDTH, 0);
   ObjectSetInteger(0, object_name, OBJPROP_SELECTABLE, false);
   ObjectSetInteger(0, object_name, OBJPROP_SELECTED, false);
   ObjectSetInteger(0, object_name, OBJPROP_RAY_RIGHT, false);
   ObjectSetInteger(0, object_name, OBJPROP_STYLE, STYLE_SOLID);
   return rectangle_handle
  }
//+------------------------------------------------------------------+
//|                                                                  |
//+------------------------------------------------------------------+
int load_classic_levels()
  {
   Print("load_tops started");
   long chart_id = ChartID();
   string chart_symbol = ChartSymbol(chart_id);
   CFileCSV fileCSV;
   string file_name = chart_symbol + ".multi_timeframe_peaks_n_valleys.csv";

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
         string timeframe = parts[0];
         string date_str = parts[1];
         double level = StringToDouble(parts[2]);
         double internal_margin = StringToDouble(parts[3]);
         double breakout_margin = StringToDouble(parts[4]);
         string activation_time_str = parts[5];
         string ttl_str = parts[6];
         string deactivated_at_str = parts[7];
         string archived_at_str = parts[8];
         int hit = StringToInteger(parts[9]);
         string is_overlap_of_str = parts[10];

         datetime date = StringToTime(date_str);
         datetime activation_time = StringToTime(activation_time_str);
         datetime ttl = StringToTime(ttl_str);
         datetime deactivated_at = NULL;
         if(len(deactivated_at_str)>0)
            deactivated_at = StringToTime(deactivated_at_str);
         datetime archived_at = NULL;
         if(len(archived_at_str)>0)
            archived_at = StringToTime(archived_at_str);
         bool is_overlap_of = StringToInteger(is_overlap_of_str) == 1;

         // Determine the box color based on timeframe
         color timeframe_color = TimeFrameColor(timeframe);

         // Calculate the end time (min of deactivated_at, ttl, current time + 1 day)
         datetime end_time = MathMin(MathMin(deactivated_at, ttl), TimeCurrent() + PeriodSeconds(PERIOD_D1));

         object_name = "BBS " + timeframe + "@" + DateTimeToString(time);
         string tooltip_text = string tooltip_text = StringFormat(
                                  object_name + "\nLevel: %d\nActivation: %s\nInternal: %d\nBreak Out: %dOverlap: %d\n", 
                                  level, activation_time_str, internal_margin, breakout_margin);
         if(hit>0)
            tooltip_text += StringFormat("Hit:%d", hit);
         Hit: %.0f\nTTL: %.0f\nDeactivated: %.0f\nArchived: %.0f\n
         tooltip_text += StringFormat("TTL:%s", ttl_str);
         if(deactivated!=NULL && )
         // Draw the colored box
         DrawColoredBox(activation_time, end_time, internal_margin, external_margin, boxColor, object_name, NULL);
         ObjectSetString(0, object_name, OBJPROP_TOOLTIP, tooltip_text);
         // Draw the blue horizontal line
         ObjectCreate(0, object_name + "-L", OBJ_TREND, 0, date, level, end_time, level);
         ObjectSetInteger(0, object_name + "-L", OBJPROP_COLOR, clrBlue);
         ObjectSetInteger(0, object_name + "-L", OBJPROP_RAY_RIGHT, true);
         ObjectSetString(0, object_name + "-L", OBJPROP_TOOLTIP, tooltip_text);


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
