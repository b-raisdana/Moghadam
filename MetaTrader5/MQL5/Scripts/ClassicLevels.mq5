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
#include <Canvas\Canvas.mqh>
#include <Timeframe.mqh>

bool classic_levels_are_loaded = false;

//+------------------------------------------------------------------+
//|                                                                  |
//+------------------------------------------------------------------+
int DrawColoredBox(datetime start, datetime end, double top, double bottom, color fill_color, string object_name)
  {
   return DrawColoredBox(start, end, top, bottom, fill_color, object_name, NULL, 0);
  }
//+------------------------------------------------------------------+
//|                                                                  |
//+------------------------------------------------------------------+
int DrawColoredBox(datetime start, datetime end, double top, double bottom, color fill_color, string object_name,
                   color border_color, long chart_id)
  {
   int rectangle_handle =ObjectCreate(chart_id, object_name, OBJ_RECTANGLE, 0, start, bottom, end, top);
   if(! rectangle_handle)
     {
      Print(__FUNCTION__,
            ": failed to create a rectangle! Error code = ",GetLastError());
      return(false);
     }
   ObjectSetInteger(chart_id, object_name, OBJPROP_FILL, clrNONE);
   if(fill_color!=NULL)
     {
      uint effective_fill_color = ColorToARGB(fill_color, 0xf0);
      ObjectSetInteger(chart_id, object_name, OBJPROP_FILL, clrNONE);//fill_color);
     }
   ObjectSetInteger(chart_id, object_name, OBJPROP_COLOR, border_color);//border_color);
   if(border_color == NULL)
      ObjectSetInteger(chart_id, object_name, OBJPROP_WIDTH, 0);
//ObjectSetInteger(chart_id, object_name, OBJPROP_SELECTABLE, false);
//ObjectSetInteger(chart_id, object_name, OBJPROP_SELECTED, false);
//ObjectSetInteger(chart_id, object_name, OBJPROP_RAY_RIGHT, false);
//ObjectSetInteger(chart_id, object_name, OBJPROP_STYLE, STYLE_SOLID);
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
         //if(line!="4H,2023-08-04 20:45:00,28800.5,28969.8,28769.572962385162,2023-08-04 20:45:00,2023-08-26 04:45:00,,,0,")
         //   continue;
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
         color timeframe_color = TimeframeColor(timeframe);

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
         DrawColoredBox(activation_time, end_time, internal_margin, breakout_margin, timeframe_color, object_name, timeframe_color, 0);
         ObjectSetString(chart_id, object_name, OBJPROP_TOOLTIP, tooltip_text);
         // Draw the blue horizontal line
         ObjectCreate(chart_id, object_name + "-L", OBJ_TREND, 0, date, level, end_time, level);
         ObjectSetInteger(chart_id, object_name + "-L", OBJPROP_COLOR, clrBlue);
         ObjectSetInteger(chart_id, object_name + "-L", OBJPROP_RAY_RIGHT, false);
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
//+------------------------------------------------------------------+
//|                                                                  |
//+------------------------------------------------------------------+
bool timeframe_legend_printed = false;
string timeframes [] =
  {
   "1min",
   "5min",
   "15min",
   "1H",
   "4H",
   "1D",
   "1W"
  };
//+------------------------------------------------------------------+
//|                                                                  |
//+------------------------------------------------------------------+
void TimeframeLegend(long chart_id)
  {
   CCanvas canvas1;
   datetime window_corner_time, window_corner_time2;
   double window_corner_price, window_corner_price2;
   int sub_windows;
   ChartXYToTimePrice(0, 0, 0, sub_windows, window_corner_time, window_corner_price);
   ChartXYToTimePrice(0, 100, 200, sub_windows, window_corner_time2, window_corner_price2);
   if(!timeframe_legend_printed)
     {
      for(int i=0; i<ArraySize(timeframes); i++)
        {
         //CCanvas canvas;
         //int x = 10, y;
         //y = 10 + i * 20;
         ////ObjectCreate(0, "Legend-"+timeframes[i],OBJ_RECTANGLE_LABEL,0, TimeCurrent());
         //if(!canvas.CreateBitmap(
         //      "Legend-"+timeframes[i],         // name
         //      TimeCurrent(),                   // time
         //      27000,                           // price
         //      200,                             // width
         //      200,                             // height
         //      COLOR_FORMAT_ARGB_NORMALIZE      // format
         //      //chart_id,                              // chart identifier
         //      //0,
         //      //"Legend-"+timeframes[i],                               // object name
         //      //200,                                 // image width in pixels
         //      //ArraySize(timeframes)*20,                                // image height in pixels
         //      //COLOR_FORMAT_ARGB_NORMALIZE       // color processing method
         //   ))
         //  {
         //   Print("Failed canvas.Attach:" + GetLastError());
         //  }
         //canvas.FillRectangle(x, y, x + 100, y+10, TimeframeColor(timeframes[i]));
         //canvas.TextOut(x, y, timeframes[i], clrBlack);
         //canvas.Update(true);
         int x = 10, y;
         y = 10 + i * 20;

         string objName = "Legend-"+timeframes[i];
         //if(!ObjectCreate(chart_id, objName, OBJ_RECTANGLE_LABEL, 0, TimeCurrent(), 27000))
         //  {
         //   Print("Failed to create object:" + GetLastError());
         //   continue;
         //  }
         //if(!(
         //      ObjectSetInteger(chart_id, objName, OBJPROP_XDISTANCE, x) &&
         //      ObjectSetInteger(chart_id, objName, OBJPROP_YDISTANCE, y) &&
         //      ObjectSetInteger(chart_id, objName, OBJPROP_XSIZE, 200) &&
         //      ObjectSetInteger(chart_id, objName, OBJPROP_YSIZE, 20) &&
         //      //ObjectSetInteger(chart_id, objName, OBJPROP_BGCOLOR, TimeframeColor(timeframes[i])) &&
         //      ObjectSetInteger(chart_id, objName, OBJPROP_COLOR, clrBlack) &&
         //      //ObjectSetInteger(chart_id, objName, OBJPROP_FILL, true) &&
         //      //ObjectSetInteger(chart_id, objName, OBJPROP_BACK, true) &&
         //      ObjectSetInteger(chart_id, objName, OBJPROP_CORNER, CORNER_LEFT_UPPER) &&
         //      ObjectSetInteger(chart_id, objName, OBJPROP_STYLE, STYLE_SOLID) &&
         //      ObjectSetInteger(0, objName, OBJPROP_SELECTED, false) &&
         //      ObjectSetInteger(0, objName, OBJPROP_SELECTABLE, false) &&
         //      ObjectSetString(chart_id, objName, OBJPROP_TEXT, timeframes[i])
         //   ))
         //  {
         //   Print("Failed to modify object:" + GetLastError());
         //  }
         if(!ObjectCreate(chart_id, objName + "-t", OBJ_LABEL, 0, 0, 0))
           {
            Print("Failed to create object:" + GetLastError());
            continue;
           }
         if(!(
               ObjectSetInteger(chart_id, objName, OBJPROP_XDISTANCE, x) &&
               ObjectSetInteger(chart_id, objName, OBJPROP_YDISTANCE, y) &&
               // ObjectSetInteger(chart_id, objName, OBJPROP_BGCOLOR, TimeframeColor(timeframes[i])) &&
               ObjectSetInteger(chart_id, objName, OBJPROP_COLOR, TimeframeColor(timeframes[i])) &&
               ObjectSetInteger(chart_id, objName, OBJPROP_FILL, true) &&
               ObjectSetInteger(chart_id, objName, OBJPROP_BACK, true) &&
               ObjectSetInteger(chart_id, objName, OBJPROP_CORNER, CORNER_LEFT_UPPER) &&
               ObjectSetInteger(chart_id, objName, OBJPROP_STYLE, STYLE_SOLID) &&
               ObjectSetInteger(0, objName, OBJPROP_SELECTED, true) &&
               ObjectSetInteger(0, objName, OBJPROP_SELECTABLE, true) &&
               ObjectSetString(chart_id, objName, OBJPROP_TEXT, timeframes[i])
            ))
           {
            Print("Failed to modify object:" + GetLastError());
           }
         PrintFormat("0x%.8X - clrBlue",TimeframeColor(timeframes[i]));
        }
     }
  }
//+------------------------------------------------------------------+
//| Expert initialization function                                   |
//+------------------------------------------------------------------+
int OnInit()
  {
   Print("ClassicLevels OnInit");
//test_draw_box();
   if(!classic_levels_are_loaded)
     {
      long chart_id = ChartID();
      TimeframeLegend(chart_id);
      //load_classic_levels("top");
      ChartRedraw();
      //load_classic_levels("bull_bear_side");
     }
   else
     {
      Print("classic_levels_are_loaded");
     }
//update_object_display();
   return(0);
  }

//+------------------------------------------------------------------+

//+------------------------------------------------------------------+
