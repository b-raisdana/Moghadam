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

#include <Canvas/Canvas.mqh>
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
                   color border_color, long chart_id)
  {
//if(!ObjectCreate(chart_ID,name,OBJ_RECTANGLE,sub_window,time1,price1,time2,price2))
//   int           x1, y1, x2, y2;
//   ChartTimePriceToXY(
//      chart_id,      // Chart ID
//      0,             // The number of the subwindow
//      start,         // Time on the chart
//      bottom,        // Price on the chart
//      x1,            // The X coordinate for the time on the chart
//      y1             // The Y coordinates for the price on the chart
//   );
//   ChartTimePriceToXY(
//      chart_id,      // Chart ID
//      0,             // The number of the subwindow
//      end,         // Time on the chart
//      top,        // Price on the chart
//      x2,            // The X coordinate for the time on the chart
//      y2             // The Y coordinates for the price on the chart
//   );
//   CCanvas box;
//   box.FillRectangle(x1, y1, x2, y2,ColorToARGB(fill_color,0x10));
////   Print("Error in DrawColoredBox:" + GetLastError() );
////}
//   return &box;

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
bool classic_levels_are_loaded = false;
void test_draw_box()
  {
   string input_text = "1H,2023-09-30 13:12:00,26907.5,26918.3,26904.72237003385,2023-09-30 13:12:00,2023-10-05 21:12:00,,,0,\n1H,2023-09-30 15:46:00,27092.0,27041.2,27100.32856322383,2023-09-30 15:46:00,2023-10-05 23:46:00,,,0,";
   string lines[];
   StringSplit(input_text, '\n', lines);
//int           x1, y1, x2, y2;
//ChartTimePriceToXY(
//   chart_id,      // Chart ID
//   0,             // The number of the subwindow
//   start,         // Time on the chart
//   bottom,        // Price on the chart
//   x1,            // The X coordinate for the time on the chart
//   y1             // The Y coordinates for the price on the chart
//);
//ChartTimePriceToXY(
//   chart_id,      // Chart ID
//   0,             // The number of the subwindow
//   end,         // Time on the chart
//   top,        // Price on the chart
//   x2,            // The X coordinate for the time on the chart
//   y2             // The Y coordinates for the price on the chart
//);
//double p1, p2;
//int sub1, sub2;
//datetime t1, t2;
//ChartXYToTimePrice(0, 0,0,sub1,t1, p1);
//ChartXYToTimePrice(0, 100,100,sub2,t2, p2);
//CCanvas box;
//box.FillRectangle(0, 0, 100, 100,clrBlue);//ColorToARGB(clrBlue,0x10));
//box.Update(true);
   datetime t1, t2;
   t1 = StringToTime("2023-09-30 13:12:00");
   t2 = StringToTime("2023-09-29 13:12:00");
   ObjectCreate(0, "TTT", OBJ_RECTANGLE, 0, t1, 26907.5, t2, 26918.3);
   DrawColoredBox(
      t1,               //datetime start,
      t2,               //datetime end,
      26907.5,          //double top,
      26918.3,          //double bottom,
      clrRed,           //color fill_color,
      "Test_Object",    //string object_name,
      clrYellow,        //color border_color,
      0                 //long chart_id)
      );
    ChartRedraw(0);
  }
//+------------------------------------------------------------------+
//|                                                                  |
//+------------------------------------------------------------------+
int OnInit()
  {
   Print("ClassicLevels OnInit");
   test_draw_box();
//   if(!classic_levels_are_loaded)
//     {
//      load_classic_levels("top");
//      //load_classic_levels("bull_bear_side");
//     }
////update_object_display();
   return(0);
  }

//+------------------------------------------------------------------+

//+------------------------------------------------------------------+
