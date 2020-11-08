//+------------------------------------------------------------------+
//|                                             JsonAPIIndicator.mq5 |
//|                                  Copyright 2020,  Gunther Schulz |
//|                                     https://www.guntherschulz.de |
//+------------------------------------------------------------------+

#property copyright "2020 Gunther Schulz"
#property link      "https://www.guntherschulz.de"
#property version   "1.00"

#include <StringToEnumInt.mqh>
#include <Zmq/Zmq.mqh>
#include <Json.mqh>

// Set ports and host for ZeroMQ
string HOST="localhost";
int CHART_SUB_PORT=15562;

// ZeroMQ Cnnections
Context context("MQL5 JSON API");
Socket chartSubscriptionSocket(context,ZMQ_SUB);

//--- input parameters
#property indicator_buffers 31
#property indicator_plots   30

input string            IndicatorId="";
input string            ShortName="JsonAPIIndicator";

//--- indicator settings
double                  B0[], B1[], B2[], B3[], B4[], B5[], B6[], B7[], B8[], B9[], B10[];
double                  B11[], B12[], B13[], B14[], B15[], B16[], B17[], B18[], B19[], B20[];
double                  B21[], B22[], B23[], B24[], B25[], B26[], B27[], B28[], B29[];
double                  alive[];
bool                    debug = false;
bool                    first = false;
int                     activeBufferCount = 0;

//+------------------------------------------------------------------+
//| Custom indicator initialization function                         |
//+------------------------------------------------------------------+
int OnInit()

  {

   bool result = chartSubscriptionSocket.connect(StringFormat("tcp://%s:%d", HOST, CHART_SUB_PORT));
   if(result == false)
     {
      Print("Failed to subscrbe on port ", CHART_SUB_PORT);
     }
   else
     {
      if(debug)
         Print("Accepting Chart Indicator data on port ", CHART_SUB_PORT);
      // TODO subscribe only to own IndicatorId topic
      // Subscribe to all topics
      chartSubscriptionSocket.setSubscribe("");
      //chartSubscriptionSocket.setLinger(1000);
      chartSubscriptionSocket.setLinger(10000);
      // Number of messages to buffer in RAM.
      chartSubscriptionSocket.setReceiveHighWaterMark(5); // TODO confirm settings
     }


//--- indicator buffers mapping;
   ArraySetAsSeries(B0,true);
   ArraySetAsSeries(B1,true);
   ArraySetAsSeries(B2,true);
   ArraySetAsSeries(B3,true);
   ArraySetAsSeries(B4,true);
   ArraySetAsSeries(B5,true);
   ArraySetAsSeries(B6,true);
   ArraySetAsSeries(B7,true);
   ArraySetAsSeries(B8,true);
   ArraySetAsSeries(B9,true);
   ArraySetAsSeries(B10,true);
   ArraySetAsSeries(B11,true);
   ArraySetAsSeries(B12,true);
   ArraySetAsSeries(B13,true);
   ArraySetAsSeries(B14,true);
   ArraySetAsSeries(B15,true);
   ArraySetAsSeries(B16,true);
   ArraySetAsSeries(B17,true);
   ArraySetAsSeries(B18,true);
   ArraySetAsSeries(B19,true);
   ArraySetAsSeries(B20,true);
   ArraySetAsSeries(B21,true);
   ArraySetAsSeries(B22,true);
   ArraySetAsSeries(B23,true);
   ArraySetAsSeries(B24,true);
   ArraySetAsSeries(B25,true);
   ArraySetAsSeries(B26,true);
   ArraySetAsSeries(B27,true);
   ArraySetAsSeries(B28,true);
   ArraySetAsSeries(B29,true);
   ArraySetAsSeries(alive,true);


   SetIndexBuffer(0,B0,INDICATOR_DATA);
   SetIndexBuffer(1,B1,INDICATOR_DATA);
   SetIndexBuffer(2,B2,INDICATOR_DATA);
   SetIndexBuffer(3,B3,INDICATOR_DATA);
   SetIndexBuffer(4,B4,INDICATOR_DATA);
   SetIndexBuffer(5,B5,INDICATOR_DATA);
   SetIndexBuffer(6,B6,INDICATOR_DATA);
   SetIndexBuffer(7,B7,INDICATOR_DATA);
   SetIndexBuffer(8,B8,INDICATOR_DATA);
   SetIndexBuffer(9,B9,INDICATOR_DATA);
   SetIndexBuffer(10,B10,INDICATOR_DATA);
   SetIndexBuffer(11,B11,INDICATOR_DATA);
   SetIndexBuffer(12,B12,INDICATOR_DATA);
   SetIndexBuffer(13,B13,INDICATOR_DATA);
   SetIndexBuffer(14,B14,INDICATOR_DATA);
   SetIndexBuffer(15,B15,INDICATOR_DATA);
   SetIndexBuffer(16,B16,INDICATOR_DATA);
   SetIndexBuffer(17,B17,INDICATOR_DATA);
   SetIndexBuffer(18,B18,INDICATOR_DATA);
   SetIndexBuffer(19,B19,INDICATOR_DATA);
   SetIndexBuffer(20,B20,INDICATOR_DATA);
   SetIndexBuffer(21,B21,INDICATOR_DATA);
   SetIndexBuffer(22,B22,INDICATOR_DATA);
   SetIndexBuffer(23,B23,INDICATOR_DATA);
   SetIndexBuffer(24,B24,INDICATOR_DATA);
   SetIndexBuffer(25,B25,INDICATOR_DATA);
   SetIndexBuffer(26,B26,INDICATOR_DATA);
   SetIndexBuffer(27,B27,INDICATOR_DATA);
   SetIndexBuffer(28,B28,INDICATOR_DATA);
   SetIndexBuffer(29,B29,INDICATOR_DATA);
   SetIndexBuffer(30,alive,INDICATOR_CALCULATIONS); // If the buffer index changes, the line starting with "CopyBuffer(chartWindowIndicators[i].indicatorHandle," in JsonAPI.mq5 has to be updated


//---
   IndicatorSetString(INDICATOR_SHORTNAME,ShortName);

   return(INIT_SUCCEEDED);
  }


//+------------------------------------------------------------------+
//|                                                                  |
//+------------------------------------------------------------------+
void SetStyle(int bufferIdx, string linelabel, color colorstyle, int linetype, int linestyle, int linewidth)
  {
   PlotIndexSetString(bufferIdx,PLOT_LABEL,linelabel);
   PlotIndexSetInteger(bufferIdx,PLOT_LINE_COLOR,0,colorstyle);
   PlotIndexSetInteger(bufferIdx,PLOT_DRAW_TYPE,linetype);
   PlotIndexSetInteger(bufferIdx,PLOT_LINE_STYLE,linestyle);
   PlotIndexSetInteger(bufferIdx,PLOT_LINE_WIDTH,linewidth);
  }

//+------------------------------------------------------------------+
//| Custom indicator iteration function                              |
//+------------------------------------------------------------------+
int OnCalculate(const int rates_total,
                const int prev_calculated,
                const datetime &time[],
                const double &open[],
                const double &high[],
                const double &low[],
                const double &close[],
                const long &tick_volume[],
                const long &volume[],
                const int &spread[])
  {

// While a new candle is forming, set the current value to be empty
   if(rates_total>prev_calculated)
     {
      B0[0] = EMPTY_VALUE;
      B1[0] = EMPTY_VALUE;
      B2[0] = EMPTY_VALUE;
      B3[0] = EMPTY_VALUE;
      B4[0] = EMPTY_VALUE;
      B5[0] = EMPTY_VALUE;
      B6[0] = EMPTY_VALUE;
      B7[0] = EMPTY_VALUE;
      B8[0] = EMPTY_VALUE;
      B9[0] = EMPTY_VALUE;
      B10[0] = EMPTY_VALUE;
      B11[0] = EMPTY_VALUE;
      B12[0] = EMPTY_VALUE;
      B13[0] = EMPTY_VALUE;
      B14[0] = EMPTY_VALUE;
      B15[0] = EMPTY_VALUE;
      B16[0] = EMPTY_VALUE;
      B17[0] = EMPTY_VALUE;
      B18[0] = EMPTY_VALUE;
      B19[0] = EMPTY_VALUE;
      B20[0] = EMPTY_VALUE;
      B21[0] = EMPTY_VALUE;
      B22[0] = EMPTY_VALUE;
      B23[0] = EMPTY_VALUE;
      B24[0] = EMPTY_VALUE;
      B25[0] = EMPTY_VALUE;
      B26[0] = EMPTY_VALUE;
      B27[0] = EMPTY_VALUE;
      B28[0] = EMPTY_VALUE;
      B29[0] = EMPTY_VALUE;
     }
   if(first==false)
      alive[0] = 1;
// ChartRedraw(0);

//--- return value of prev_calculated for next call
   return(rates_total);
  }

//+------------------------------------------------------------------+
//|                                                                  |
//+------------------------------------------------------------------+
void SubscriptionHandler(ZmqMsg &chartMsg)
  {
   CJAVal message;
// Get data from request
   string msg=chartMsg.getData();
   if(debug)
      Print("Processing:"+msg);
// Deserialize msg to CJAVal array
   if(!message.Deserialize(msg))
     {
      Alert("Deserialization Error");
      ExpertRemove();
     }
   if(message["indicatorChartId"]==IndicatorId)
     {

      if(message["action"]=="PLOT" && message["actionType"]=="DATA")
        {
         int bufferIdx = message["indicatorBufferId"].ToInt();
         if(bufferIdx == 0)
            WriteToBuffer(message, B0);
         if(bufferIdx == 1)
            WriteToBuffer(message, B1);
         if(bufferIdx == 2)
            WriteToBuffer(message, B2);
         if(bufferIdx == 3)
            WriteToBuffer(message, B3);
         if(bufferIdx == 4)
            WriteToBuffer(message, B4);
         if(bufferIdx == 5)
            WriteToBuffer(message, B5);
         if(bufferIdx == 6)
            WriteToBuffer(message, B6);
         if(bufferIdx == 7)
            WriteToBuffer(message, B7);
         if(bufferIdx == 8)
            WriteToBuffer(message, B8);
         if(bufferIdx == 9)
            WriteToBuffer(message, B9);
         if(bufferIdx == 10)
            WriteToBuffer(message, B10);
         if(bufferIdx == 11)
            WriteToBuffer(message, B11);
         if(bufferIdx == 12)
            WriteToBuffer(message, B12);
         if(bufferIdx == 13)
            WriteToBuffer(message, B13);
         if(bufferIdx == 14)
            WriteToBuffer(message, B14);
         if(bufferIdx == 15)
            WriteToBuffer(message, B15);
         if(bufferIdx == 16)
            WriteToBuffer(message, B16);
         if(bufferIdx == 17)
            WriteToBuffer(message, B17);
         if(bufferIdx == 18)
            WriteToBuffer(message, B18);
         if(bufferIdx == 19)
            WriteToBuffer(message, B19);
         if(bufferIdx == 20)
            WriteToBuffer(message, B20);
         if(bufferIdx == 21)
            WriteToBuffer(message, B21);
         if(bufferIdx == 22)
            WriteToBuffer(message, B22);
         if(bufferIdx == 23)
            WriteToBuffer(message, B23);
         if(bufferIdx == 24)
            WriteToBuffer(message, B24);
         if(bufferIdx == 25)
            WriteToBuffer(message, B25);
         if(bufferIdx == 26)
            WriteToBuffer(message, B26);
         if(bufferIdx == 27)
            WriteToBuffer(message, B27);
         if(bufferIdx == 28)
            WriteToBuffer(message, B28);
         if(bufferIdx == 29)
            WriteToBuffer(message, B29);
        }
      else
         if(message["action"]=="PLOT" && message["actionType"]=="ADDBUFFER")
           {
            string linelabel = message["style"]["linelabel"].ToStr();
            string colorstyleStr = message["style"]["color"].ToStr();
            string linetypeStr = message["style"]["linetype"].ToStr();
            string linestyleStr = message["style"]["linestyle"].ToStr();
            int linewidth = message["style"]["linewidth"].ToInt();

            color colorstyle = StringToColor(colorstyleStr);
            int linetype = StringToEnumInt(linetypeStr);
            int linestyle = StringToEnumInt(linestyleStr);

            /*
            //if (aa == false) {
            Print("SETBUFF ActCount  ",activeBufferCount);
            if (activeBufferCount == 0) {SetIndexBuffer(0,B1,INDICATOR_DATA);} // Two semicolons ar required! No idea why. Seems to be a timing problem, better to keep it in init()

            if (activeBufferCount == 1) {SetIndexBuffer(1,B2,INDICATOR_DATA);;}
            if (activeBufferCount == 2) {SetIndexBuffer(2,B3,INDICATOR_DATA);;}
            if (activeBufferCount == 3) {SetIndexBuffer(3,B4,INDICATOR_DATA);;}
            if (activeBufferCount == 4) {SetIndexBuffer(4,B5,INDICATOR_DATA);;}

            //aa = true;}
            */

            SetStyle(activeBufferCount, linelabel, colorstyle, linetype, linestyle, linewidth);
            activeBufferCount = activeBufferCount + 1;
           }
     }
  }

//+------------------------------------------------------------------+
//| Update indicator buffer function                                 |
//+------------------------------------------------------------------+
void WriteToBuffer(CJAVal &message, double &buffer[])
  {

   int bufferSize = ArraySize(buffer);

   int messageDataSize = message["data"].Size();
// TODO check if this is working as expected. Seems to
   if(first==false)
     {
      for(int i=0; i<activeBufferCount; i++)
        {
         //Print("BUFF ",bufferSize-messageDataSize, " ",ArraySize(B2)," ", ArraySize(B3), " ",messageDataSize);
         PlotIndexSetInteger(i,PLOT_DRAW_BEGIN,bufferSize-messageDataSize);
        }
      first = true;
     }

   for(int i=0; i<messageDataSize; i++)
     {
      // don't add more elements than the automatically sized buffer array can hold
      if(i+1<bufferSize)
        {
         // the first element is the current unformed candle, so we start at index 1
         // we reverse the order of the incoming values, which are expected to be ascending
         //buffer[i+1] = message["data"][messageDataSize-1-i].ToDbl();
         buffer[i+1] = message["data"][messageDataSize-1-i].ToDbl();
        }
     }
// Set the most recent plotted value to nothing, as we do not have any data for yet unformed candles
   buffer[0] = EMPTY_VALUE;
  }


//+------------------------------------------------------------------+
//| Check for new indicator data function                            |
//+------------------------------------------------------------------+
void CheckMessages()
  {
// This is a workaround for Timer(). It is needed, because OnTimer() works if the indicator is manually added to a chart, but not with ChartIndicatorAdd()

   ZmqMsg chartMsg;

// Recieve chart instructions stream from client via live Chart socket.
   chartSubscriptionSocket.recv(chartMsg,true);

// Request recieved
   if(chartMsg.size()>0)
     {
      // Handle subscription SubscriptionHandler()
      SubscriptionHandler(chartMsg);
      ChartRedraw(ChartID());
     }
  }

//+------------------------------------------------------------------+
//| OnTimer() workaround function                                    |
//+------------------------------------------------------------------+
// Gets triggered by the OnTimer() function of the JsonAPI Expert script
void OnChartEvent(const int id,
                  const long &lparam,
                  const double &dparam,
                  const string &sparam)
  {
   if(id==CHARTEVENT_CUSTOM+222)
      CheckMessages();
  }
//+----------------------------------------------------
