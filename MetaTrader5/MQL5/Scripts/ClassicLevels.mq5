//+------------------------------------------------------------------+
//|                                              ClassicLevels.mq5 |
//|                                                          Behrooz |
//|                                             https://www.mql5.com |
//+------------------------------------------------------------------+
#property copyright "Behrooz"
#property link      "https://www.mql5.com"
#property version   "1.00"

#include <FileCSV.mqh>
#include <Arrays\ArrayString.mqh>
#include <Timeframe.mqh>


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
int DrawColoredBox(datetime start, datetime end, double top, double bottom, color fill_color, string object_name)
  {
   return DrawColoredBox(start, end, top, bottom, fill_color, object_name, NULL, 0);
  }
//+------------------------------------------------------------------+
//|                                                                  |
//+------------------------------------------------------------------+
int DrawColoredBox(datetime start, datetime end, double top, double bottom, color fill_color, string object_name,
                   color border_color, int chart_id)
  {
   int rectangle_handle = ObjectCreate(chart_id, object_name, OBJ_RECTANGLE_LABEL, 0, start, bottom, end, top);
   ObjectSetInteger(chart_id, object_name, OBJPROP_FILL, fill_color);
   ObjectSetInteger(chart_id, object_name, OBJPROP_COLOR, border_color);
   if(border_color == NULL)
      ObjectSetInteger(chart_id, object_name, OBJPROP_WIDTH, 1);
   ObjectSetInteger(chart_id, object_name, OBJPROP_SELECTABLE, false);
   ObjectSetInteger(chart_id, object_name, OBJPROP_SELECTED, false);
   ObjectSetInteger(chart_id, object_name, OBJPROP_RAY_RIGHT, false);
   ObjectSetInteger(chart_id, object_name, OBJPROP_STYLE, STYLE_SOLID);
   return rectangle_handle;
  }

//+------------------------------------------------------------------+
//|                                                                  |
//+------------------------------------------------------------------+
int load_classic_levels(string level_type)
  {
   Print("load_classic_levels started");
   long chart_id = ChartID();
   string chart_symbol = ChartSymbol(chart_id);
   CFileCSV fileCSV;
   string file_name = chart_symbol + ".multi_timeframe_"+level_type+"_pivots.csv";

   datetime start_time = NULL;
   datetime end_time = NULL;
   int counter = 0;

   if(fileCSV.Open(file_name, FILE_READ|FILE_CSV|FILE_ANSI))
     {
      string line;
      fileCSV.Read(line); // Read the header
      if(line != "timeframe,date,level,internal_margin,external_margin,activation_time,ttl,deactivated_at,archived_at,hit,is_overlap_of")
        {
         Print("Invalid input file format in "+ file_name +":" +line);
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
         if(StringLen(deactivated_at_str)>0)
            deactivated_at = StringToTime(deactivated_at_str);
         datetime archived_at = NULL;
         if(StringLen(archived_at_str)>0)
            archived_at = StringToTime(archived_at_str);
         bool is_overlap_of = StringToInteger(is_overlap_of_str) == 1;

         // Determine the box color based on timeframe
         color timeframe_color = TimeFrameColor(timeframe);

         // Calculate the end time (min of deactivated_at, ttl, current time + 1 day)
         if(deactivated_at==NULL)
            deactivated_at = TimeCurrent() + PeriodSeconds(PERIOD_D1);
         datetime end_time = MathMin(deactivated_at, ttl);

         string object_name;
         if(level_type=="bull_bear_side")
            object_name = "BBS " + timeframe + "@" + date_str;
         else
            if(level_type=="top")
               object_name = "TOP " + timeframe + "@" + date_str;
            else
              {
               Print("Invalid level_type:" + level_type);
               return false;
              }
         string tooltip_text = StringFormat(
                                  object_name + "\nLevel: %d\nActivation: %s\nInternal: %d\nBreak Out: %dOverlap: %d\n",
                                  level, activation_time_str, internal_margin, breakout_margin);
         if(hit>0)
            tooltip_text += StringFormat("Hit: %d", hit);
         tooltip_text += StringFormat("TTL:%s", ttl_str);
         if(StringLen(deactivated_at_str)>0)
            tooltip_text += StringFormat("Deactivated: %s", deactivated_at_str);
         if(StringLen(archived_at_str)>0)
            tooltip_text += StringFormat("Archived: %s", archived_at_str);
         // Draw the colored box
         DrawColoredBox(activation_time, end_time, internal_margin, breakout_margin, clrBlue, object_name, NULL, chart_id);
         ObjectSetString(chart_id, object_name, OBJPROP_TOOLTIP, tooltip_text);
         // Draw the blue horizontal line
         ObjectCreate(chart_id, object_name + "-L", OBJ_TREND, 0, date, level, end_time, level);
         ObjectSetInteger(chart_id, object_name + "-L", OBJPROP_COLOR, clrBlue);
         ObjectSetInteger(chart_id, object_name + "-L", OBJPROP_RAY_RIGHT, true);
         ObjectSetString(chart_id, object_name + "-L", OBJPROP_TOOLTIP, tooltip_text);
         SetObjectTimeframes(object_name);
         counter++;
        }
      fileCSV.Close();
      classic_levels_are_loaded = true;
      Print(IntegerToString(counter) + " Tops added: " + TimeToString(start_time) +"-"+TimeToString(end_time));
     }
   else
     {
      Print("Failed to open file. Error code: ", GetLastError(), "Filename:"+file_name);
     }
   return 1;
  }
//+------------------------------------------------------------------+
//|                                                                  |
//+------------------------------------------------------------------+
bool classic_levels_are_loaded = false;
int OnInit()
  {
   Print("ClassicLevels OnInit");
   if(!classic_levels_are_loaded)
     {
      load_classic_levels("top");
      //load_classic_levels("bull_bear_side");
     }
//update_object_display();
   return(0);
  }

//+------------------------------------------------------------------+

//+------------------------------------------------------------------+
