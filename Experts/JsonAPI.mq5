//+------------------------------------------------------------------+
//
// Copyright (C) 2019 Nikolai Khramkov
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.
//
//+------------------------------------------------------------------+

// TODO: Deviation

#property copyright   "Copyright 2019, Nikolai Khramkov."
#property link        "https://github.com/khramkov"
#property version     "2.00"
#property description "MQL5 JSON API"
#property description "See github link for documentation"

#include <Trade/AccountInfo.mqh>
#include <Trade/DealInfo.mqh>
#include <Trade/Trade.mqh>
#include <Zmq/Zmq.mqh>
#include <Json.mqh>
#include <StringToEnumInt.mqh>
#include <ControlErrors.mqh>

// Set ports and host for ZeroMQ
string HOST="*";
int SYS_PORT=15555;
int DATA_PORT=15556;
int LIVE_PORT=15557;
int STR_PORT=15558;
int INDICATOR_DATA_PORT=15559;
int CHART_DATA_PORT=15560;
int CHART_INDICATOR_DATA_PORT=15562;

// ZeroMQ Cnnections
Context context("MQL5 JSON API");
Socket sysSocket(context,ZMQ_REP);
Socket dataSocket(context,ZMQ_PUSH);
Socket liveSocket(context,ZMQ_PUSH);
Socket streamSocket(context,ZMQ_PUSH);
Socket indicatorDataSocket(context,ZMQ_PUSH);
Socket chartDataSocket(context,ZMQ_PULL);
Socket chartIndicatorDataSocket(context,ZMQ_PUB);

// Global variables \\
bool debug = false;
bool liveStream = true;
bool connectedFlag = true;
int deInitReason = -1;
double chartAttached = ChartID(); // Chart id where the expert is attached to

// Variables for handling price data stream
struct SymbolSubscription
  {
   string            symbol;
   string            chartTf;
   datetime          lastBar;
  };
SymbolSubscription symbolSubscriptions[];
int symbolSubscriptionCount = 0;

// Variables for controlling indicators
struct Indicator
  {
   long              id; // Internal id
   string            indicatorId; // UUID
   int               indicatorHandle; // Internal id/handle
   int               indicatorParamCount; // Number of parameters to be passed to the indicator
   int               indicatorBufferCount; // Numnber of buffers to be returned bythe indicator
  };
Indicator indicators[];
int indicatorCount = 0;

// Variables for controlling chart
struct ChartWindow
  {
   long              id; // Internal id
   string            chartId; // UUID
  };
ChartWindow chartWindows[];
int chartWindowCount = 0;

struct ChartWindowIndicator
  {
   long              id; // Internal id
   string            indicatorId; // UUID
   int               indicatorHandle; // Internal id/handle
  };

ChartWindowIndicator chartWindowIndicators[];
int chartWindowIndicatorCount = 0;

// Error handling
ControlErrors mControl;

//+------------------------------------------------------------------+
//| Bind ZMQ sockets to ports                                        |
//+------------------------------------------------------------------+
bool BindSockets()
  {
   sysSocket.setLinger(1000);
   dataSocket.setLinger(1000);
   liveSocket.setLinger(1000);
   streamSocket.setLinger(1000);
   indicatorDataSocket.setLinger(1000);
   chartDataSocket.setLinger(1000);
   chartIndicatorDataSocket.setLinger(1000);

// Number of messages to buffer in RAM.
   sysSocket.setSendHighWaterMark(1000);
   dataSocket.setSendHighWaterMark(1000);
   liveSocket.setSendHighWaterMark(1000);
   streamSocket.setSendHighWaterMark(1000);
   indicatorDataSocket.setSendHighWaterMark(1000);
   chartDataSocket.setReceiveHighWaterMark(1000); // TODO confirm settings
   chartIndicatorDataSocket.setReceiveHighWaterMark(1000);
   
   bool result = false;
   result = sysSocket.bind(StringFormat("tcp://%s:%d", HOST,SYS_PORT));
   if(result == false)
     {
      return result;
     }
   else
     {
      Print("Bound 'System' socket on port ", SYS_PORT);
     }
   result = dataSocket.bind(StringFormat("tcp://%s:%d", HOST,DATA_PORT));
   if(result == false)
     {
      return result;
     }
   else
     {
      Print("Bound 'Data' socket on port ", DATA_PORT);
     }
   result = liveSocket.bind(StringFormat("tcp://%s:%d", HOST,LIVE_PORT));
   if(result == false)
     {
      return result;
     }
   else
     {
      Print("Bound 'Live' socket on port ", LIVE_PORT);
     }
   result = streamSocket.bind(StringFormat("tcp://%s:%d", HOST,STR_PORT));
   if(result == false)
     {
      return result;
     }
   else
     {
      Print("Bound 'Streaming' socket on port ", STR_PORT);
     }
   result = indicatorDataSocket.bind(StringFormat("tcp://%s:%d", HOST,INDICATOR_DATA_PORT));
   if(result == false)
     {
      return result;
     }
   else
     {
      Print("Bound 'Indicator Data' socket on port ", INDICATOR_DATA_PORT);
     }
   result = chartDataSocket.bind(StringFormat("tcp://%s:%d", HOST,CHART_DATA_PORT));
   if(result == false)
     {
      return result;
     }
   else
     {
      Print("Bound 'Chart Data' socket on port ", CHART_DATA_PORT);
     }
   result = chartIndicatorDataSocket.bind(StringFormat("tcp://%s:%d", HOST,CHART_INDICATOR_DATA_PORT));
   if(result == false)
     {
      return result;
     }
   else
     {
      Print("Bound 'JsonAPIIndicator Data' socket on port ", CHART_INDICATOR_DATA_PORT);
     }

   return result;
  }

//+------------------------------------------------------------------+
//| Expert initialization function                                   |
//+------------------------------------------------------------------+
int OnInit()
  {

// Setting up error reporting
   mControl.SetAlert(true);
   mControl.SetSound(false);
   mControl.SetWriteFlag(false);

   /* Bindinig ZMQ ports on init */
// Skip reloading of the EA script when the reason to reload is a chart timeframe change
   if(deInitReason != REASON_CHARTCHANGE)
     {

      EventSetMillisecondTimer(1);

      int bindSocketsDelay = 65; // Seconds to wait if binding of sockets fails.
      int bindAttemtps = 2; // Number of binding attemtps

      Print("Binding sockets...");

      for(int i=0; i<bindAttemtps; i++)
        {
         if(BindSockets())
            return(INIT_SUCCEEDED);
         else
           {
            Print("Binding sockets failed. Waiting ", bindSocketsDelay, " seconds to try again...");
            Sleep(bindSocketsDelay*1000);
           }
        }

      Print("Binding of sockets failed permanently.");
      return(INIT_FAILED);
     }

//testDraw();
   return(INIT_SUCCEEDED);
  }

//+------------------------------------------------------------------+
//| Expert deinitialization function                                 |
//+------------------------------------------------------------------+
void OnDeinit(const int reason)
  {
   /* Unbinding ZMQ ports on denit */

// TODO Ports do not get freed immediately under Wine. How to properly close ports? There is a timeout of about 60 sec.
// https://forum.winehq.org/viewtopic.php?t=22758
// https://github.com/zeromq/cppzmq/issues/139

   deInitReason = reason;

// Skip reloading of the EA script when the reason to reload is a chart timeframe change
   if(reason != REASON_CHARTCHANGE)
     {
      Print(__FUNCTION__," Deinitialization reason: ", getUninitReasonText(reason));

      Print("Unbinding 'System' socket on port ", SYS_PORT, "..");
      sysSocket.unbind(StringFormat("tcp://%s:%d", HOST, SYS_PORT));
      Print("Unbinding 'Data' socket on port ", DATA_PORT, "..");
      dataSocket.unbind(StringFormat("tcp://%s:%d", HOST, DATA_PORT));
      Print("Unbinding 'Live' socket on port ", LIVE_PORT, "..");
      liveSocket.unbind(StringFormat("tcp://%s:%d", HOST, LIVE_PORT));
      Print("Unbinding 'Streaming' socket on port ", STR_PORT, "..");
      streamSocket.unbind(StringFormat("tcp://%s:%d", HOST, STR_PORT));
      Print("Unbinding 'Chart Data' socket on port ", STR_PORT, "..");
      streamSocket.unbind(StringFormat("tcp://%s:%d", HOST, CHART_DATA_PORT));
      Print("Unbinding 'JsonAPIIndicator Data' socket on port ", STR_PORT, "..");
      streamSocket.unbind(StringFormat("tcp://%s:%d", HOST, CHART_INDICATOR_DATA_PORT));

      // Shutdown ZeroMQ Context
      context.shutdown();
      context.destroy(0);

      // Reset
      ResetSubscriptionsAndIndicators();

      EventKillTimer();
     }
  }

//+------------------------------------------------------------------+
//| Check if subscribed to symbol and timeframe combination          |
//+------------------------------------------------------------------+
bool HasChartSymbol(string symbol, string chartTF)
  {
   for(int i=0; i<ArraySize(symbolSubscriptions); i++)
     {
      if(symbolSubscriptions[i].symbol == symbol && symbolSubscriptions[i].chartTf == chartTF)
        {
         return true;
        }
     }
   return false;
  }

//+------------------------------------------------------------------+
//| Get index of indicator handler array by indicator id string      |
//+------------------------------------------------------------------+
int GetIndicatorIdxByIndicatorId(string indicatorId)
  {
   for(int i=0; i<indicatorCount; i++)
     {
      if(indicators[i].indicatorId == indicatorId)
        {
         return i;
        }
     }
   return -1;
  }

//+------------------------------------------------------------------+
//| Get index of chart window array by chart window id string        |
//+------------------------------------------------------------------+
int GetChartWindowIdxByChartWindowId(string chartWindowId)
  {
   for(int i=0; i<chartWindowCount; i++)
     {
      if(chartWindows[i].chartId == chartWindowId)
        {
         return i;
        }
     }
   return -1;
  }

//+------------------------------------------------------------------+
//| Get index of chart indicator handler array by indicator id string      |
//+------------------------------------------------------------------+
int GetChartIndicatorIdxByChartIndicatorId(string indicatorId)
  {
   for(int i=0; i<chartWindowIndicatorCount; i++)
     {
      if(chartWindowIndicators[i].indicatorId == indicatorId)
        {
         return i;
        }
     }
   return -1;
  }


//+------------------------------------------------------------------+
//| Stream live price data                                           |
//+------------------------------------------------------------------+
void StreamPriceData()
  {
// If liveStream == true, push last candle to liveSocket.

   if(liveStream)
     {
      CJAVal last;
      if(TerminalInfoInteger(TERMINAL_CONNECTED))
        {
         connectedFlag=true;
         for(int i=0; i<symbolSubscriptionCount; i++)
           {
            string symbol=symbolSubscriptions[i].symbol;
            string chartTF=symbolSubscriptions[i].chartTf;
            datetime lastBar=symbolSubscriptions[i].lastBar;
            CJAVal Data;
            ENUM_TIMEFRAMES period = GetTimeframe(chartTF);

            datetime thisBar = 0;
            float price;
            MqlTick tick;
            MqlRates rates[1];
            int spread[1];

            if(chartTF == "TICK")
              {
               if(SymbolInfoTick(symbol,tick) !=true) { /*mControl.Check();*/ }
               thisBar=(datetime) tick.time_msc;
              }
            else
              {
               if(CopyRates(symbol,period,1,1,rates)!=1) { /*mControl.Check();*/ }
               if(CopySpread(symbol,period,1,1,spread)!=1) { /*mControl.Check();*/; }
               thisBar=(datetime)rates[0].time;
              }

            if(lastBar!=thisBar)
              {
               if(lastBar!=0)  // skip first price data after startup/reset
                 {
                  if(chartTF == "TICK")
                    {
                     Data[0] = (long)    tick.time_msc;
                     Data[1] = (double)  tick.bid;
                     Data[2] = (double)  tick.ask;
                    }
                  else
                    {
                     Data[0] = (long) rates[0].time;
                     Data[1] = (double) rates[0].open;
                     Data[2] = (double) rates[0].high;
                     Data[3] = (double) rates[0].low;
                     Data[4] = (double) rates[0].close;
                     Data[5] = (double) rates[0].tick_volume;
                     Data[6] = (int) spread[0];
                    }
                  last["status"] = (string) "CONNECTED";
                  last["symbol"] = (string) symbol;
                  last["timeframe"] = (string) chartTF;
                  last["data"].Set(Data);

                  string t=last.Serialize();
                  if(debug)
                     Print(t);
                  InformClientSocket(liveSocket,t);
                  symbolSubscriptions[i].lastBar=thisBar;

                 }
               else
                  symbolSubscriptions[i].lastBar=thisBar;
              }
           }
        }
      else
        {
         // send disconnect message only once
         if(connectedFlag)
           {
            last["status"] = (string) "DISCONNECTED";
            string t=last.Serialize();
            if(debug)
               Print(t);
            InformClientSocket(liveSocket,t);
            connectedFlag=false;
           }
        }
     }
  }

//+------------------------------------------------------------------+
//| Expert timer function                                            |
//+------------------------------------------------------------------+
void OnTimer()
  {

// Stream live price data
   StreamPriceData();

   ZmqMsg request;

// Get request from client via System socket.
   sysSocket.recv(request,true);

// Request recived
   if(request.size()>0)
     {
      // Pull request to RequestHandler().
      RequestHandler(request);
     }

// Publish indicator values for the JsonAPIIndicator indicator
   ZmqMsg chartMsg;
   chartDataSocket.recv(chartMsg, true);
   if(chartMsg.size()>0)
     {
      double values[];
      // Ensure that all indicators have finished intitailisation
      for(int i=0; i<ArraySize(chartWindowIndicators); i++)
        {
         //CopyBuffer(chartWindowIndicators[i].indicatorHandle, 7, 0, 1, values); // '7' is the number of the 'alive' indicator buffer
         // Wait for CopyBuffer to return. Ensures that indicator has been initialized
         CopyBuffer(chartWindowIndicators[i].indicatorHandle, 0, 0, 1, values); // '7' is the number of the 'alive' indicator buffer
        }
      chartIndicatorDataSocket.send(chartMsg,true);
     }

// Trigger the indicator JsonAPIIndicator to check for new Messages
   for(int i=0; i<ArraySize(chartWindows); i++)
     {
      long chartId = chartWindows[i].id;
      EventChartCustom(chartId, 222, 222, 222.0);
     }
  }

//+------------------------------------------------------------------+
//| Request handler                                                  |
//+------------------------------------------------------------------+
void RequestHandler(ZmqMsg &request)
  {

   CJAVal incomingMessage;

   ResetLastError();
// Get data from reguest
   string msg=request.getData();

   if(debug)
      Print("Processing:"+msg);

   if(!incomingMessage.Deserialize(msg))
     {
      mControl.mSetUserError(65537, GetErrorID(65537));
      CheckError(__FUNCTION__);
     }

// Send response to System socket that request was received
// Some historical data requests can take a lot of time
   InformClientSocket(sysSocket, "OK");

// Process action command
   string action = incomingMessage["action"].ToStr();

   if(action=="CONFIG")
      ScriptConfiguration(incomingMessage);
   else
      if(action=="ACCOUNT")
         GetAccountInfo();
      else
         if(action=="BALANCE")
            GetBalanceInfo();
         else
            if(action=="HISTORY")
               HistoryInfo(incomingMessage);
            else
               if(action=="TRADE")
                  TradingModule(incomingMessage);
               else
                  if(action=="POSITIONS")
                     GetPositions(incomingMessage);
                  else
                     if(action=="ORDERS")
                        GetOrders(incomingMessage);
                     else
                        if(action=="RESET")
                           ResetSubscriptionsAndIndicators();
                        else
                           if(action=="INDICATOR")
                              IndicatorControl(incomingMessage);
                           else
                              if(action=="CHART")
                                 ChartControl(incomingMessage);
                              else
                                {
                                 mControl.mSetUserError(65538, GetErrorID(65538));
                                 CheckError(__FUNCTION__);
                                }

  }

//+------------------------------------------------------------------+
//| Reconfigure the script params                                    |
//+------------------------------------------------------------------+
void ScriptConfiguration(CJAVal &dataObject)
  {

   string symbol=dataObject["symbol"].ToStr();
   string chartTF=dataObject["chartTF"].ToStr();

   ArrayResize(symbolSubscriptions, symbolSubscriptionCount+1);
   symbolSubscriptions[symbolSubscriptionCount].symbol = symbol;
   symbolSubscriptions[symbolSubscriptionCount].chartTf = chartTF;
// to initialze with value 0 skips the first price
   symbolSubscriptions[symbolSubscriptionCount].lastBar = 0;
   symbolSubscriptionCount++;

   mControl.mResetLastError();
   SymbolInfoString(symbol, SYMBOL_DESCRIPTION);
   if(!CheckError(__FUNCTION__))
      ActionDoneOrError(ERR_SUCCESS, __FUNCTION__, "ERR_SUCCESS");
  }

//+------------------------------------------------------------------+
//| Start new indicator or request indicator data                    |
//+------------------------------------------------------------------+
void IndicatorControl(CJAVal &dataObject)
  {

   string actionType=dataObject["actionType"].ToStr();

   if(actionType=="REQUEST")
     {
      GetIndicatorResult(dataObject);
     }
   else
      if(actionType=="ATTACH")
        {
         StartIndicator(dataObject);
        }
  }

//+------------------------------------------------------------------+
//| Check if string is a representation of a number                  |
//+------------------------------------------------------------------+
bool IsNumberAsString(string str)
  {
// MQL5 seems to return true if the values are the same, no matter the data type, in this case comparing str and dbl/int.
// (str "2.1" == double 2.1) will return true.
   double dbl = StringToDouble(str);
   int integer = StringToInteger(str);
// Compaing to both int and double to cover both cases
   if(str==dbl || str==integer)
      return true;
   else
      return false;
  }

//+------------------------------------------------------------------+
//| Start new indicator instance                                     |
//+------------------------------------------------------------------+
void StartIndicator(CJAVal &dataObject)
  {

// TODO map Indicators Constants https://www.mql5.com/en/docs/constants/indicatorconstants

   string symbol=dataObject["symbol"].ToStr();
   string chartTF=dataObject["chartTF"].ToStr();
   string id=dataObject["id"].ToStr();
   string indicatorName=dataObject["name"].ToStr();

   indicatorCount++;
   ArrayResize(indicators,indicatorCount);

   int idx = indicatorCount-1;

   indicators[idx].indicatorId = id;
   indicators[idx].indicatorBufferCount = dataObject["linecount"].ToInt();

   double params[];
   indicators[idx].indicatorParamCount = dataObject["params"].Size();
   for(int i=0; i<indicators[idx].indicatorParamCount; i++)
     {
      // TODO test it. Is it ok to pass EnumInts as Doubles for params?
      ArrayResize(params, i+1);
      string paramStr = dataObject["params"][i].ToStr();
      if(IsNumberAsString(paramStr))
         params[i] = StringToDouble(paramStr);
      else
        {
         params[i] = StringToEnumInt(paramStr);
         mControl.mResetLastError(); // TODO find where the Error 4003 is craeted in StringToEnumInt
        }
     }

   ENUM_TIMEFRAMES period = GetTimeframe(chartTF);

// Case construct for passing variable parameter count to the iCustom function is used, because MQL5 does not seem to support expanding an array to a function parameter list
   switch(indicators[idx].indicatorParamCount)
     {
      case 0:
         indicators[idx].indicatorHandle = iCustom(symbol,period,indicatorName);
         break;
      case 1:
         indicators[idx].indicatorHandle = iCustom(symbol,period,indicatorName, params[0]);
         break;
      case 2:
         indicators[idx].indicatorHandle = iCustom(symbol,period,indicatorName, params[0], params[1]);
         break;
      case 3:
         indicators[idx].indicatorHandle = iCustom(symbol,period,indicatorName, params[0], params[1], params[2]);
         break;
      case 4:
         indicators[idx].indicatorHandle = iCustom(symbol,period,indicatorName, params[0], params[1], params[2], params[3]);
         break;
      case 5:
         indicators[idx].indicatorHandle = iCustom(symbol,period,indicatorName, params[0], params[1], params[2], params[3], params[4]);
         break;
      case 6:
         indicators[idx].indicatorHandle = iCustom(symbol,period,indicatorName, params[0], params[1], params[2], params[3], params[4], params[5]);
         break;
      case 7:
         indicators[idx].indicatorHandle = iCustom(symbol,period,indicatorName, params[0], params[1], params[2], params[3], params[4], params[5], params[6]);
         break;
      case 8:
         indicators[idx].indicatorHandle = iCustom(symbol,period,indicatorName, params[0], params[1], params[2], params[3], params[4], params[5], params[6], params[7]);
         break;
      case 9:
         indicators[idx].indicatorHandle = iCustom(symbol,period,indicatorName, params[0], params[1], params[2], params[3], params[4], params[5], params[6], params[7], params[8]);
         break;
      case 10:
         indicators[idx].indicatorHandle = iCustom(symbol,period,indicatorName, params[0], params[1], params[2], params[3], params[4], params[5], params[6], params[7], params[8], params[9]);
         break;
      default:
         // TODO error handling
         break;
     }

   CJAVal message;

   if(mControl.mGetLastError())
     {
      int lastError = mControl.mGetLastError();
      string desc = mControl.mGetDesc();
      mControl.Check();

      message["error"]=(bool) true;
      message["lastError"]=(string) lastError;
      message["description"]=desc;
      message["function"]=(string) __FUNCTION__;
      string t=message.Serialize();
      if(debug)
         Print(t);
      InformClientSocket(indicatorDataSocket,t);
     }
   else
     {
      message["error"]=(bool) false;
      message["id"] = (string) id;

      string t=message.Serialize();
      if(debug)
         Print(t);
      InformClientSocket(indicatorDataSocket,t);
     }

  }

//+------------------------------------------------------------------+
//| Get indicator results                                            |
//+------------------------------------------------------------------+
void GetIndicatorResult(CJAVal &dataObject)
  {

   datetime fromDate=dataObject["fromDate"].ToInt();
   string id=dataObject["id"].ToStr();
   string indicatorName=dataObject["indicatorName"].ToStr();

   int idx = GetIndicatorIdxByIndicatorId(id);

   double values[2];

   CJAVal results;
// Cycle through all avaliable buffer positions
   for(int i=0; i<indicators[idx].indicatorBufferCount; i++)
     {
      values[0] = 0.0;
      values[1] = 0.0;
      results[i] = 0.0;
      if(idx >= 0)
        {
         if(CopyBuffer(indicators[idx].indicatorHandle, i, fromDate, 1, values) < 0)
           {
            if(mControl.mGetLastError())
              {
               CJAVal message;
               int lastError = mControl.mGetLastError();
               string desc = mControl.mGetDesc();
               mControl.Check();

               message["error"]=(bool) true;
               message["lastError"]=(string) lastError;
               message["description"]=desc;
               message["function"]=(string) __FUNCTION__;
               string t=message.Serialize();
               if(debug)
                  Print(t);
               InformClientSocket(indicatorDataSocket,t);
              }
           }
         results[i] = DoubleToString(values[0]);
        }
     }

   CJAVal message;
   message["error"]=(bool) false;
   message["id"] = (string) id;
   message["data"].Set(results);

   string t=message.Serialize();
   if(debug)
      Print(t);
   InformClientSocket(indicatorDataSocket,t);

  }

//+------------------------------------------------------------------+
//| Open new chart or add indicator to chart                         |
//+------------------------------------------------------------------+

//+------------------------------------------------------------------+
//|                                                                  |
//+------------------------------------------------------------------+
void ChartControl(CJAVal &dataObject)
  {

   string actionType=dataObject["actionType"].ToStr();

   if(actionType=="ADDINDICATOR")
     {
      AddChartIndicator(dataObject);
     }
   else
      if(actionType=="OPEN")
        {
         OpenChart(dataObject);
        }
  }

//+------------------------------------------------------------------+
//| Open new chart                                                   |
//+------------------------------------------------------------------+
void OpenChart(CJAVal &dataObject)
  {

   string chartId=dataObject["chartId"].ToStr();
   string symbol=dataObject["symbol"].ToStr();
   string chartTF=dataObject["chartTF"].ToStr();

   chartWindowCount++;
   ArrayResize(chartWindows,chartWindowCount);

   int idx = chartWindowCount-1;

   chartWindows[idx].chartId = chartId;

   ENUM_TIMEFRAMES period = GetTimeframe(chartTF);
   chartWindows[idx].id = ChartOpen(symbol, period);
   ChartSetInteger(chartWindows[idx].id, CHART_AUTOSCROLL, false);

   CJAVal message;
   message["error"]=(bool) false;
   message["chartId"] = (string) chartId;
   message["mtChartId"] = (string) chartWindows[idx].id;

   string t=message.Serialize();
   if(debug)
      Print(t);
   InformClientSocket(dataSocket,t);
  }

//+------------------------------------------------------------------+
//| Add JsonAPIIndicator indicator to chart                          |
//+------------------------------------------------------------------+
void AddChartIndicator(CJAVal &dataObject)
  {

   string chartIdStr=dataObject["chartId"].ToStr();
   string chartIndicatorId=dataObject["chartIndicatorId"].ToStr();
   int chartIndicatorSubWindow=dataObject["chartIndicatorSubWindow"].ToInt();
   string shortName = dataObject["shortName"].ToStr();

   int chartIdx = GetChartWindowIdxByChartWindowId(chartIdStr);
   long chartId = chartWindows[chartIdx].id;

   double chartIndicatorHandle = iCustom(ChartSymbol(chartId),ChartPeriod(chartId),"JsonAPIIndicator",chartIndicatorId,shortName); //linelabel,colorstyle,linetype,linestyle,linewidth);

   if(ChartIndicatorAdd(chartId, chartIndicatorSubWindow, chartIndicatorHandle))
     {
      chartWindowIndicatorCount++;
      ArrayResize(chartWindowIndicators,chartWindowIndicatorCount);
      int indicatorIdx = chartWindowIndicatorCount-1;
      chartWindowIndicators[indicatorIdx].indicatorId = chartIndicatorId;
      chartWindowIndicators[indicatorIdx].indicatorHandle = chartIndicatorHandle;
     }
   if(!CheckError(__FUNCTION__))
     {
      CJAVal message;
      message["error"]=(bool) false;
      message["chartId"] = (string) chartIdStr;

      string t=message.Serialize();
      if(debug)
         Print(t);
      InformClientSocket(dataSocket,t);
     }
  }

//+------------------------------------------------------------------+
//| Account information                                              |
//+------------------------------------------------------------------+
void GetAccountInfo()
  {

   CJAVal info;

   info["error"] = false;
   info["broker"] = AccountInfoString(ACCOUNT_COMPANY);
   info["currency"] = AccountInfoString(ACCOUNT_CURRENCY);
   info["server"] = AccountInfoString(ACCOUNT_SERVER);
   info["trading_allowed"] = TerminalInfoInteger(TERMINAL_TRADE_ALLOWED);
   info["bot_trading"] = AccountInfoInteger(ACCOUNT_TRADE_EXPERT);
   info["balance"] = AccountInfoDouble(ACCOUNT_BALANCE);
   info["equity"] = AccountInfoDouble(ACCOUNT_EQUITY);
   info["margin"] = AccountInfoDouble(ACCOUNT_MARGIN);
   info["margin_free"] = AccountInfoDouble(ACCOUNT_MARGIN_FREE);
   info["margin_level"] = AccountInfoDouble(ACCOUNT_MARGIN_LEVEL);

   string t=info.Serialize();
   if(debug)
      Print(t);
   InformClientSocket(dataSocket,t);
  }

//+------------------------------------------------------------------+
//| Balance information                                              |
//+------------------------------------------------------------------+
void GetBalanceInfo()
  {

   CJAVal info;
   info["balance"] = AccountInfoDouble(ACCOUNT_BALANCE);
   info["equity"] = AccountInfoDouble(ACCOUNT_EQUITY);
   info["margin"] = AccountInfoDouble(ACCOUNT_MARGIN);
   info["margin_free"] = AccountInfoDouble(ACCOUNT_MARGIN_FREE);

   string t=info.Serialize();
   if(debug)
      Print(t);
   InformClientSocket(dataSocket,t);
  }

//+------------------------------------------------------------------+
//| Push historical data to ZMQ socket                               |
//+------------------------------------------------------------------+
bool PushHistoricalData(CJAVal &data)
  {
   string t=data.Serialize();
   if(debug)
      Print(t);
   InformClientSocket(dataSocket,t);
   return true;
  }

//+------------------------------------------------------------------+
//| Correct historical tick data                                     |
//+------------------------------------------------------------------+
// Some brokers (markets.com) deliver incorrect historical tick data
// with an incorrect spread. 
// This attempts to automatically adjust the
// historical tick data.
// Live bar data is also represented with the same, incorrect spread so we attempt
// to adjust historical tick data by that same amount.
void CorrectTicks(string symbol, MqlTick &copyTicksArray[])
  {
   MqlTick symbolInfoTick;
   double offsetBid, offsetAsk;
   int tickCount = ArraySize(copyTicksArray);
   SymbolInfoTick(symbol,symbolInfoTick);

   offsetBid = copyTicksArray[tickCount-1].bid - symbolInfoTick.bid;
   offsetAsk = copyTicksArray[tickCount-1].ask - symbolInfoTick.ask;

   for(int i=0; i<tickCount; i++)
     {
      copyTicksArray[i].bid = copyTicksArray[i].bid - offsetBid;
      copyTicksArray[i].ask = copyTicksArray[i].ask - offsetAsk;
     }
  }

//+------------------------------------------------------------------+
//| Get historical data                                              |
//+------------------------------------------------------------------+
void HistoryInfo(CJAVal &dataObject)
  {

   string actionType = dataObject["actionType"].ToStr();
   string chartTF = dataObject["chartTF"].ToStr();
   string symbol=dataObject["symbol"].ToStr();
   bool correctTickHistory=dataObject["correctTickHistory"].ToBool();

// Write CVS fle to local directory
   if(actionType=="WRITE" && chartTF=="TICK")
     {

      CJAVal data, d, msg;
      MqlTick tickArray[];
      string fileName=symbol + "-" + chartTF + ".csv";  // file name
      string directoryName="Data"; // directory name
      string outputFile=directoryName+"\\"+fileName;

      ENUM_TIMEFRAMES period=GetTimeframe(chartTF);
      datetime fromDate=(datetime)dataObject["fromDate"].ToInt();
      datetime toDate=TimeCurrent();
      if(dataObject["toDate"].ToInt()!=NULL)
         toDate=(datetime)dataObject["toDate"].ToInt();

      Print("Fetching HISTORY");
      Print("1) Symbol: "+symbol);
      Print("2) Timeframe: Ticks");
      Print("3) Date from: "+TimeToString(fromDate));
      if(dataObject["toDate"].ToInt()!=NULL)
         Print("4) Date to:"+TimeToString(toDate));

      int tickCount = 0;
      ulong fromDateM = StringToTime(fromDate);
      ulong toDateM = StringToTime(toDate);

      tickCount=CopyTicksRange(symbol,tickArray,COPY_TICKS_ALL,1000*(ulong)fromDateM,1000*(ulong)toDateM);
      if(tickCount < 0)
        {
         mControl.mSetUserError(65541, GetErrorID(65541));
        }
      CheckError(__FUNCTION__);

      Print("Preparing data of ", tickCount, " ticks for ", symbol);
      if(correctTickHistory)
         CorrectTicks(symbol,tickArray);
      int file_handle=FileOpen(outputFile, FILE_WRITE | FILE_CSV);
      if(file_handle!=INVALID_HANDLE)
        {
         msg["status"] = (string) "CONNECTED";
         msg["type"] = (string) "NORMAL";
         msg["data"] = (string) StringFormat("Writing to: %s\\%s", TerminalInfoString(TERMINAL_DATA_PATH), outputFile);
         if(liveStream)
            InformClientSocket(liveSocket, msg.Serialize());
         ActionDoneOrError(ERR_SUCCESS, __FUNCTION__, "ERR_SUCCESS");
         //ActionDoneOrError(ERR_SUCCESS  , __FUNCTION__, dataSocket);
         // Inform client that file is avalable for writing

         PrintFormat("%s file is available for writing",fileName);
         PrintFormat("File path: %s\\Files\\",TerminalInfoString(TERMINAL_DATA_PATH));
         //--- write the time and values of signals to the file
         for(int i=0; i<tickCount; i++)
           {
            FileWrite(file_handle,tickArray[i].time_msc, ",", tickArray[i].bid, ",", tickArray[i].ask);
            msg["status"] = (string) "CONNECTED";
            msg["type"] = (string) "FLUSH";
            msg["data"] = (string) tickArray[i].time_msc;
            if(liveStream)
               InformClientSocket(liveSocket, msg.Serialize());
           }
         //--- close the file
         FileClose(file_handle);
         PrintFormat("Data is written, %s file is closed",fileName);
         msg["status"] = (string) "DISCONNECTED";
         msg["type"] = (string) "NORMAL";
         msg["data"] = (string) StringFormat("Writing to: %s\\%s", outputFile, " is finished");
         if(liveStream)
            InformClientSocket(liveSocket, msg.Serialize());

        }
      else
        {
         // File is not available for writing
         mControl.mSetUserError(65542, GetErrorID(65542));
         CheckError(__FUNCTION__);
        }
      connectedFlag=false;
     }

// Write CVS fle to local directory
   else
      if(actionType=="WRITE" && chartTF!="TICK")
        {

         CJAVal c, d;
         MqlRates r[];
         int spread[];
         string fileName=symbol + "-" + chartTF + ".csv";  // file name
         string directoryName="Data"; // directory name
         string outputFile=directoryName+"//"+fileName;

         int barCount;
         ENUM_TIMEFRAMES period=GetTimeframe(chartTF);
         datetime fromDate=(datetime)dataObject["fromDate"].ToInt();
         datetime toDate=TimeCurrent();
         if(dataObject["toDate"].ToInt()!=NULL)
            toDate=(datetime)dataObject["toDate"].ToInt();

         Print("Fetching HISTORY");
         Print("1) Symbol :"+symbol);
         Print("2) Timeframe :"+EnumToString(period));
         Print("3) Date from :"+TimeToString(fromDate));
         if(dataObject["toDate"].ToInt()!=NULL)
            Print("4) Date to:"+TimeToString(toDate));

         barCount=CopyRates(symbol,period,fromDate,toDate,r);
         if(CopySpread(symbol,period, fromDate, toDate, spread)!=1)
           {
            mControl.mSetUserError(65541, GetErrorID(65541));
           }

         Print("Preparing tick data of ", barCount, " ticks for ", symbol);
         int file_handle=FileOpen(outputFile, FILE_WRITE | FILE_CSV);
         if(file_handle!=INVALID_HANDLE)
           {
            ActionDoneOrError(ERR_SUCCESS, __FUNCTION__, "ERR_SUCCESS");;
            PrintFormat("%s file is available for writing",outputFile);
            PrintFormat("File path: %s\\Files\\",TerminalInfoString(TERMINAL_DATA_PATH));
            //--- write the time and values of signals to the file
            for(int i=0; i<barCount; i++)
               FileWrite(file_handle,r[i].time, ",", r[i].open, ",", r[i].high, ",", r[i].low, ",", r[i].close, ",", r[i].tick_volume, spread[i]);
            //--- close the file
            FileClose(file_handle);
            PrintFormat("Data is written, %s file is closed", outputFile);
           }
         else
           {
            mControl.mSetUserError(65542, GetErrorID(65542));
            CheckError(__FUNCTION__);
           }
        }

      else
         if(actionType=="DATA" && chartTF=="TICK")
           {

            CJAVal data, d;
            MqlTick tickArray[];

            ENUM_TIMEFRAMES period=GetTimeframe(chartTF);
            datetime fromDate=(datetime)dataObject["fromDate"].ToInt();
            datetime toDate=TimeCurrent();
            if(dataObject["toDate"].ToInt()!=NULL)
               toDate=(datetime)dataObject["toDate"].ToInt();

            if(debug)
              {
               Print("Fetching HISTORY");
               Print("1) Symbol: "+symbol);
               Print("2) Timeframe: Ticks");
               Print("3) Date from: "+TimeToString(fromDate));
               if(dataObject["toDate"].ToInt()!=NULL)
                  Print("4) Date to:"+TimeToString(toDate));
              }

            int tickCount = 0;
            ulong fromDateM = StringToTime(fromDate);
            ulong toDateM = StringToTime(toDate);

            tickCount=CopyTicksRange(symbol,tickArray, COPY_TICKS_ALL, 1000*(ulong)fromDateM, 1000*(ulong)toDateM);
            Print("Preparing tick data of ", tickCount, " ticks for ", symbol);
            if(correctTickHistory)
               CorrectTicks(symbol,tickArray);
            if(tickCount)
              {
               for(int i=0; i<tickCount; i++)
                 {
                  data[i][0]=(long)   tickArray[i].time_msc;
                  data[i][1]=(double) tickArray[i].bid;
                  data[i][2]=(double) tickArray[i].ask;
                 }
               d["data"].Set(data);
              }
            else
              {
               d["data"].Add(data);
              }
            Print("Finished preparing tick data");

            d["symbol"]=symbol;
            d["timeframe"]=chartTF;

            PushHistoricalData(d);
           }

         else
            if(actionType=="DATA" && chartTF!="TICK")
              {

               CJAVal c, d;
               MqlRates r[];
               int spread[];
               int barCount=0;
               ENUM_TIMEFRAMES period=GetTimeframe(chartTF);
               datetime fromDate=(datetime)dataObject["fromDate"].ToInt();
               datetime toDate=TimeCurrent();
               if(dataObject["toDate"].ToInt()!=NULL)
                  toDate=(datetime)dataObject["toDate"].ToInt();

               if(debug)
                 {
                  Print("Fetching HISTORY");
                  Print("1) Symbol :"+symbol);
                  Print("2) Timeframe :"+EnumToString(period));
                  Print("3) Date from :"+TimeToString(fromDate));
                  if(dataObject["toDate"].ToInt()!=NULL)
                     Print("4) Date to:"+TimeToString(toDate));
                 }

               barCount=CopyRates(symbol, period, fromDate, toDate, r);
               if(CopySpread(symbol,period, fromDate, toDate, spread)!=1) { /*mControl.Check();*/ }

               if(barCount)
                 {
                  for(int i=0; i<barCount; i++)
                    {
                     c[i][0]=(long)   r[i].time;
                     c[i][1]=(double) r[i].open;
                     c[i][2]=(double) r[i].high;
                     c[i][3]=(double) r[i].low;
                     c[i][4]=(double) r[i].close;
                     c[i][5]=(double) r[i].tick_volume;
                     c[i][6]=(int) spread[i];
                    }
                  d["data"].Set(c);
                 }
               else
                 {
                  d["data"].Add(c);
                 }

               d["symbol"]=symbol;
               d["timeframe"]=chartTF;

               PushHistoricalData(d);
              }

            else
               if(actionType=="TRADES")
                 {
                  CDealInfo tradeInfo;
                  CJAVal trades, data;

                  if(HistorySelect(0,TimeCurrent()))
                    {
                     // Get total deals in history
                     int total = HistoryDealsTotal();
                     ulong ticket; // deal ticket

                     for(int i=0; i<total; i++)
                       {
                        if((ticket=HistoryDealGetTicket(i))>0)
                          {
                           tradeInfo.Ticket(ticket);
                           data["ticket"]=(long) tradeInfo.Ticket();
                           data["time"]=(long) tradeInfo.Time();
                           data["price"]=(double) tradeInfo.Price();
                           data["volume"]=(double) tradeInfo.Volume();
                           data["symbol"]=(string) tradeInfo.Symbol();
                           data["type"]=(string) tradeInfo.TypeDescription();
                           data["entry"]=(long) tradeInfo.Entry();
                           data["profit"]=(double) tradeInfo.Profit();

                           trades["trades"].Add(data);
                          }
                       }
                    }
                  else
                    {
                     trades["trades"].Add(data);
                    }

                  string t=trades.Serialize();
                  if(debug)
                     Print(t);
                  InformClientSocket(dataSocket,t);
                 }
               else
                 {
                  mControl.mSetUserError(65538, GetErrorID(65538));
                  CheckError(__FUNCTION__);
                 }
  }

//+------------------------------------------------------------------+
//| Fetch positions information                                      |
//+------------------------------------------------------------------+
void GetPositions(CJAVal &dataObject)
  {
   CPositionInfo myposition;
   CJAVal data, position;

// Get positions
   int positionsTotal=PositionsTotal();
// Create empty array if no positions
   if(!positionsTotal)
      data["positions"].Add(position);
// Go through positions in a loop
   for(int i=0; i<positionsTotal; i++)
     {
      mControl.mResetLastError();

      if(myposition.Select(PositionGetSymbol(i)))
        {
         position["id"]=PositionGetInteger(POSITION_IDENTIFIER);
         position["magic"]=PositionGetInteger(POSITION_MAGIC);
         position["symbol"]=PositionGetString(POSITION_SYMBOL);
         position["type"]=EnumToString(ENUM_POSITION_TYPE(PositionGetInteger(POSITION_TYPE)));
         position["time_setup"]=PositionGetInteger(POSITION_TIME);
         position["open"]=PositionGetDouble(POSITION_PRICE_OPEN);
         position["stoploss"]=PositionGetDouble(POSITION_SL);
         position["takeprofit"]=PositionGetDouble(POSITION_TP);
         position["volume"]=PositionGetDouble(POSITION_VOLUME);

         data["error"]=(bool) false;
         data["positions"].Add(position);
        }
      CheckError(__FUNCTION__);
     }

   string t=data.Serialize();
   if(debug)
      Print(t);
   InformClientSocket(dataSocket,t);
  }

//+------------------------------------------------------------------+
//| Fetch orders information                                         |
//+------------------------------------------------------------------+
void GetOrders(CJAVal &dataObject)
  {
   mControl.mResetLastError();

   COrderInfo myorder;
   CJAVal data, order;

// Get orders
   if(HistorySelect(0,TimeCurrent()))
     {
      int ordersTotal = OrdersTotal();
      // Create empty array if no orders
      if(!ordersTotal)
        {
         data["error"]=(bool) false;
         data["orders"].Add(order);
        }

      for(int i=0; i<ordersTotal; i++)
        {
         if(myorder.Select(OrderGetTicket(i)))
           {
            order["id"]=(string) myorder.Ticket();
            order["magic"]=OrderGetInteger(ORDER_MAGIC);
            order["symbol"]=OrderGetString(ORDER_SYMBOL);
            order["type"]=EnumToString(ENUM_ORDER_TYPE(OrderGetInteger(ORDER_TYPE)));
            order["time_setup"]=OrderGetInteger(ORDER_TIME_SETUP);
            order["open"]=OrderGetDouble(ORDER_PRICE_OPEN);
            order["stoploss"]=OrderGetDouble(ORDER_SL);
            order["takeprofit"]=OrderGetDouble(ORDER_TP);
            order["volume"]=OrderGetDouble(ORDER_VOLUME_INITIAL);

            data["error"]=(bool) false;
            data["orders"].Add(order);
           }
         // Error handling
         CheckError(__FUNCTION__);
        }
     }

   string t=data.Serialize();
   if(debug)
      Print(t);
   InformClientSocket(dataSocket,t);
  }

//+------------------------------------------------------------------+
//| Trading module                                                   |
//+------------------------------------------------------------------+
void TradingModule(CJAVal &dataObject)
  {
   mControl.mResetLastError();
   CTrade trade;

   string   actionType = dataObject["actionType"].ToStr();
   string   symbol=dataObject["symbol"].ToStr();
   SymbolInfoString(symbol, SYMBOL_DESCRIPTION);
   CheckError(__FUNCTION__);

   int      idNimber=dataObject["id"].ToInt();
   double   volume=dataObject["volume"].ToDbl();
   double   SL=dataObject["stoploss"].ToDbl();
   double   TP=dataObject["takeprofit"].ToDbl();
   double   price=NormalizeDouble(dataObject["price"].ToDbl(),_Digits);
   double   deviation=dataObject["deviation"].ToDbl();
   string   comment=dataObject["comment"].ToStr();

// Order expiration section
   ENUM_ORDER_TYPE_TIME exp_type = ORDER_TIME_GTC;
   datetime expiration = 0;
   if(dataObject["expiration"].ToInt() != 0)
     {
      exp_type = ORDER_TIME_SPECIFIED;
      expiration=dataObject["expiration"].ToInt();
     }

// Market orders
   if(actionType=="ORDER_TYPE_BUY" || actionType=="ORDER_TYPE_SELL")
     {
      ENUM_ORDER_TYPE orderType=ORDER_TYPE_BUY;
      price = SymbolInfoDouble(symbol,SYMBOL_ASK);
      if(actionType=="ORDER_TYPE_SELL")
        {
         orderType=ORDER_TYPE_SELL;
         price=SymbolInfoDouble(symbol,SYMBOL_BID);
        }

      if(trade.PositionOpen(symbol,orderType,volume,price,SL,TP,comment))
        {
         OrderDoneOrError(false, __FUNCTION__, trade);
         return;
        }
     }

// Pending orders
   else
      if(actionType=="ORDER_TYPE_BUY_LIMIT" || actionType=="ORDER_TYPE_SELL_LIMIT" || actionType=="ORDER_TYPE_BUY_STOP" || actionType=="ORDER_TYPE_SELL_STOP")
        {
         if(actionType=="ORDER_TYPE_BUY_LIMIT")
           {
            if(trade.BuyLimit(volume,price,symbol,SL,TP,ORDER_TIME_GTC,expiration,comment))
              {
               OrderDoneOrError(false, __FUNCTION__, trade);
               return;
              }
           }
         else
            if(actionType=="ORDER_TYPE_SELL_LIMIT")
              {
               if(trade.SellLimit(volume,price,symbol,SL,TP,ORDER_TIME_GTC,expiration,comment))
                 {
                  OrderDoneOrError(false, __FUNCTION__, trade);
                  return;
                 }
              }
            else
               if(actionType=="ORDER_TYPE_BUY_STOP")
                 {
                  if(trade.BuyStop(volume,price,symbol,SL,TP,ORDER_TIME_GTC,expiration,comment))
                    {
                     OrderDoneOrError(false, __FUNCTION__, trade);
                     return;
                    }
                 }
               else
                  if(actionType=="ORDER_TYPE_SELL_STOP")
                    {
                     if(trade.SellStop(volume,price,symbol,SL,TP,ORDER_TIME_GTC,expiration,comment))
                       {
                        OrderDoneOrError(false, __FUNCTION__, trade);
                        return;
                       }
                    }
        }
      // Position modify
      else
         if(actionType=="POSITION_MODIFY")
           {
            if(trade.PositionModify(idNimber,SL,TP))
              {
               OrderDoneOrError(false, __FUNCTION__, trade);
               return;
              }
           }
         // Position close partial
         else
            if(actionType=="POSITION_PARTIAL")
              {
               if(trade.PositionClosePartial(idNimber,volume))
                 {
                  OrderDoneOrError(false, __FUNCTION__, trade);
                  return;
                 }
              }
            // Position close by id
            else
               if(actionType=="POSITION_CLOSE_ID")
                 {
                  if(trade.PositionClose(idNimber))
                    {
                     OrderDoneOrError(false, __FUNCTION__, trade);
                     return;
                    }
                 }
               // Position close by symbol
               else
                  if(actionType=="POSITION_CLOSE_SYMBOL")
                    {
                     if(trade.PositionClose(symbol))
                       {
                        OrderDoneOrError(false, __FUNCTION__, trade);
                        return;
                       }
                    }
                  // Modify pending order
                  else
                     if(actionType=="ORDER_MODIFY")
                       {
                        if(trade.OrderModify(idNimber,price,SL,TP,ORDER_TIME_GTC,expiration))
                          {
                           OrderDoneOrError(false, __FUNCTION__, trade);
                           return;
                          }
                       }
                     // Cancel pending order
                     else
                        if(actionType=="ORDER_CANCEL")
                          {
                           if(trade.OrderDelete(idNimber))
                             {
                              OrderDoneOrError(false, __FUNCTION__, trade);
                              return;
                             }
                          }
                        // Action type dosen't exist
                        else
                          {
                           mControl.mSetUserError(65538, GetErrorID(65538));
                           CheckError(__FUNCTION__);
                          }

// This part of the code runs if order was not completed
   OrderDoneOrError(true, __FUNCTION__, trade);
  }

//+------------------------------------------------------------------+
//| TradeTransaction function                                        |
//+------------------------------------------------------------------+
void OnTradeTransaction(const MqlTradeTransaction &trans,
                        const MqlTradeRequest &request,
                        const MqlTradeResult &result)
  {

   ENUM_TRADE_TRANSACTION_TYPE  trans_type=trans.type;
   switch(trans.type)
     {
      case  TRADE_TRANSACTION_REQUEST:
        {
         CJAVal data, req, res;

         req["action"]=EnumToString(request.action);
         req["order"]=(int) request.order;
         req["symbol"]=(string) request.symbol;
         req["volume"]=(double) request.volume;
         req["price"]=(double) request.price;
         req["stoplimit"]=(double) request.stoplimit;
         req["sl"]=(double) request.sl;
         req["tp"]=(double) request.tp;
         req["deviation"]=(int) request.deviation;
         req["type"]=EnumToString(request.type);
         req["type_filling"]=EnumToString(request.type_filling);
         req["type_time"]=EnumToString(request.type_time);
         req["expiration"]=(int) request.expiration;
         req["comment"]=(string) request.comment;
         req["position"]=(int) request.position;
         req["position_by"]=(int) request.position_by;

         res["retcode"]=(int) result.retcode;
         res["result"]=(string) GetRetcodeID(result.retcode);
         res["deal"]=(int) result.order;
         res["order"]=(int) result.order;
         res["volume"]=(double) result.volume;
         res["price"]=(double) result.price;
         res["comment"]=(string) result.comment;
         res["request_id"]=(int) result.request_id;
         res["retcode_external"]=(int) result.retcode_external;

         data["request"].Set(req);
         data["result"].Set(res);

         string t=data.Serialize();
         if(debug)
            Print(t);
         InformClientSocket(streamSocket,t);
        }
      break;
      default:
        {} break;
     }
  }

//+------------------------------------------------------------------+
//| Convert chart timeframe from string to enum                      |
//+------------------------------------------------------------------+
ENUM_TIMEFRAMES GetTimeframe(string chartTF)
  {

   ENUM_TIMEFRAMES tf;
   tf=NULL;

   if(chartTF=="TICK")
      tf=PERIOD_CURRENT;

   if(chartTF=="M1")
      tf=PERIOD_M1;

   if(chartTF=="M5")
      tf=PERIOD_M5;

   if(chartTF=="M15")
      tf=PERIOD_M15;

   if(chartTF=="M30")
      tf=PERIOD_M30;

   if(chartTF=="H1")
      tf=PERIOD_H1;

   if(chartTF=="H2")
      tf=PERIOD_H2;

   if(chartTF=="H3")
      tf=PERIOD_H3;

   if(chartTF=="H4")
      tf=PERIOD_H4;

   if(chartTF=="H6")
      tf=PERIOD_H6;

   if(chartTF=="H8")
      tf=PERIOD_H8;

   if(chartTF=="H12")
      tf=PERIOD_H12;

   if(chartTF=="D1")
      tf=PERIOD_D1;

   if(chartTF=="W1")
      tf=PERIOD_W1;

   if(chartTF=="MN1")
      tf=PERIOD_MN1;

//if tf == NULL an error will be raised in config function
   return(tf);
  }

//+------------------------------------------------------------------+
//| Trade confirmation                                               |
//+------------------------------------------------------------------+
void OrderDoneOrError(bool error, string funcName, CTrade &trade)
  {

   CJAVal conf;

   conf["error"]=(bool) error;
   conf["retcode"]=(int) trade.ResultRetcode();
   conf["desription"]=(string) GetRetcodeID(trade.ResultRetcode());
// conf["deal"]=(int) trade.ResultDeal();
   conf["order"]=(int) trade.ResultOrder();
   conf["volume"]=(double) trade.ResultVolume();
   conf["price"]=(double) trade.ResultPrice();
   conf["bid"]=(double) trade.ResultBid();
   conf["ask"]=(double) trade.ResultAsk();
   conf["function"]=(string) funcName;

   string t=conf.Serialize();
   if(debug)
      Print(t);
   InformClientSocket(dataSocket,t);
  }


//+------------------------------------------------------------------+
//| Error reporting                                                  |
//+------------------------------------------------------------------+
bool CheckError(string funcName)
  {
   int lastError = mControl.mGetLastError();
   if(lastError)
     {
      string desc = mControl.mGetDesc();
      if(debug)
         Print("Error handling source: ", funcName," description: ", desc);
      Print("Error handling source: ", funcName," description: ", desc);
      mControl.Check();
      ActionDoneOrError(lastError, funcName, desc);
      return true;
     }
   else
      return false;

  }

//+------------------------------------------------------------------+
//| Action confirmation                                              |
//+------------------------------------------------------------------+
void ActionDoneOrError(int lastError, string funcName, string desc)
  {

   CJAVal conf;

   conf["error"]=(bool)true;
   if(lastError==0)
      conf["error"]=(bool)false;

   conf["lastError"]=(string) lastError;
   conf["description"]=(string) desc;
   conf["function"]=(string) funcName;

   string t=conf.Serialize();
   if(debug)
      Print(t);
   InformClientSocket(dataSocket,t);
  }

//+------------------------------------------------------------------+
//| Inform Client via socket                                         |
//+------------------------------------------------------------------+
void InformClientSocket(Socket &workingSocket,string replyMessage)
  {

// non-blocking
   workingSocket.send(replyMessage,true);
// TODO: Array out of range error
   mControl.mResetLastError();
//mControl.Check();
  }

//+------------------------------------------------------------------+
//| Clear symbol subscriptions and indicators                        |
//+------------------------------------------------------------------+
void ResetSubscriptionsAndIndicators()
  {

   ArrayFree(symbolSubscriptions);
   symbolSubscriptionCount=0;

   bool error = false;
   for(int i=0; i<indicatorCount; i++)
     {
      if(!IndicatorRelease(indicators[i].indicatorHandle))
         error = true;
     }
   ArrayFree(indicators);
   indicatorCount = 0;

   for(int i=0; i<chartWindowIndicatorCount; i++)
     {
      if(!IndicatorRelease(chartWindowIndicators[i].indicatorHandle))
         error = true;
     }
   ArrayFree(chartWindowIndicators);
   chartWindowIndicatorCount = 0;

   for(int i=0; i<ArraySize(chartWindows); i++)
     {
      // TODO check if chart exists first: if(ChartGetInteger...
      //if(!IndicatorRelease(chartWindows[i].indicatorHandle)) error = true;
      if(chartWindows[i].id != 0)
         ChartClose(chartWindows[i].id);
     }
   ArrayFree(chartWindows);

   /*
   if(ArraySize(symbolSubscriptions)!=0 || ArraySize(indicators)!=0 || ArraySize(chartWindows)!=0 || error){
     // Set to only Alert. Fails too often, this happens when i.e. the backtrader script gets aborted unexpectedly
     mControl.Check();
     mControl.mSetUserError(65540, GetErrorID(65540));
     CheckError(__FUNCTION__);
   }
   */
   ActionDoneOrError(ERR_SUCCESS, __FUNCTION__, "ERR_SUCCESS");
  }

//+------------------------------------------------------------------+
//| Get retcode message by retcode id                                |
//+------------------------------------------------------------------+
string GetRetcodeID(int retcode)
  {

   switch(retcode)
     {
      case 10004:
         return("TRADE_RETCODE_REQUOTE");
         break;
      case 10006:
         return("TRADE_RETCODE_REJECT");
         break;
      case 10007:
         return("TRADE_RETCODE_CANCEL");
         break;
      case 10008:
         return("TRADE_RETCODE_PLACED");
         break;
      case 10009:
         return("TRADE_RETCODE_DONE");
         break;
      case 10010:
         return("TRADE_RETCODE_DONE_PARTIAL");
         break;
      case 10011:
         return("TRADE_RETCODE_ERROR");
         break;
      case 10012:
         return("TRADE_RETCODE_TIMEOUT");
         break;
      case 10013:
         return("TRADE_RETCODE_INVALID");
         break;
      case 10014:
         return("TRADE_RETCODE_INVALID_VOLUME");
         break;
      case 10015:
         return("TRADE_RETCODE_INVALID_PRICE");
         break;
      case 10016:
         return("TRADE_RETCODE_INVALID_STOPS");
         break;
      case 10017:
         return("TRADE_RETCODE_TRADE_DISABLED");
         break;
      case 10018:
         return("TRADE_RETCODE_MARKET_CLOSED");
         break;
      case 10019:
         return("TRADE_RETCODE_NO_MONEY");
         break;
      case 10020:
         return("TRADE_RETCODE_PRICE_CHANGED");
         break;
      case 10021:
         return("TRADE_RETCODE_PRICE_OFF");
         break;
      case 10022:
         return("TRADE_RETCODE_INVALID_EXPIRATION");
         break;
      case 10023:
         return("TRADE_RETCODE_ORDER_CHANGED");
         break;
      case 10024:
         return("TRADE_RETCODE_TOO_MANY_REQUESTS");
         break;
      case 10025:
         return("TRADE_RETCODE_NO_CHANGES");
         break;
      case 10026:
         return("TRADE_RETCODE_SERVER_DISABLES_AT");
         break;
      case 10027:
         return("TRADE_RETCODE_CLIENT_DISABLES_AT");
         break;
      case 10028:
         return("TRADE_RETCODE_LOCKED");
         break;
      case 10029:
         return("TRADE_RETCODE_FROZEN");
         break;
      case 10030:
         return("TRADE_RETCODE_INVALID_FILL");
         break;
      case 10031:
         return("TRADE_RETCODE_CONNECTION");
         break;
      case 10032:
         return("TRADE_RETCODE_ONLY_REAL");
         break;
      case 10033:
         return("TRADE_RETCODE_LIMIT_ORDERS");
         break;
      case 10034:
         return("TRADE_RETCODE_LIMIT_VOLUME");
         break;
      case 10035:
         return("TRADE_RETCODE_INVALID_ORDER");
         break;
      case 10036:
         return("TRADE_RETCODE_POSITION_CLOSED");
         break;
      case 10038:
         return("TRADE_RETCODE_INVALID_CLOSE_VOLUME");
         break;
      case 10039:
         return("TRADE_RETCODE_CLOSE_ORDER_EXIST");
         break;
      case 10040:
         return("TRADE_RETCODE_LIMIT_POSITIONS");
         break;
      case 10041:
         return("TRADE_RETCODE_REJECT_CANCEL");
         break;
      case 10042:
         return("TRADE_RETCODE_LONG_ONLY");
         break;
      case 10043:
         return("TRADE_RETCODE_SHORT_ONLY");
         break;
      case 10044:
         return("TRADE_RETCODE_CLOSE_ONLY");
         break;

      default:
         return("TRADE_RETCODE_UNKNOWN="+IntegerToString(retcode));
         break;
     }
  }

//+------------------------------------------------------------------+
//| Get error message by error id                                    |
//+------------------------------------------------------------------+
string GetErrorID(int error)
  {

   switch(error)
     {
      /*
      case 0:     return("ERR_SUCCESS");                        break;
      case 4301:  return("ERR_MARKET_UNKNOWN_SYMBOL");          break;
      case 4303:  return("ERR_MARKET_WRONG_PROPERTY");          break;
      case 4752:  return("ERR_TRADE_DISABLED");                 break;
      case 4753:  return("ERR_TRADE_POSITION_NOT_FOUND");       break;
      case 4754:  return("ERR_TRADE_ORDER_NOT_FOUND");          break;
      */
      // Custom errors
      case 65537:
         return("ERR_DESERIALIZATION");
         break;
      case 65538:
         return("ERR_WRONG_ACTION");
         break;
      case 65539:
         return("ERR_WRONG_ACTION_TYPE");
         break;
      case 65540:
         return("ERR_CLEAR_SUBSCRIPTIONS_FAILED");
         break;
      case 65541:
         return("ERR_RETRIEVE_DATA_FAILED");
         break;
      case 65542:
         return("ERR_CVS_FILE_CREATION_FAILED");
         break;


      default:
         return("ERR_CODE_UNKNOWN="+IntegerToString(error));
         break;
     }
  }

//+------------------------------------------------------------------+
//| Return a textual description of the deinitialization reason code |
//+------------------------------------------------------------------+
string getUninitReasonText(int reasonCode)
  {
   string text="";
//---
   switch(reasonCode)
     {
      case REASON_ACCOUNT:
         text="Account was changed";
         break;
      case REASON_CHARTCHANGE:
         text="Symbol or timeframe was changed";
         break;
      case REASON_CHARTCLOSE:
         text="Chart was closed";
         break;
      case REASON_PARAMETERS:
         text="Input-parameter was changed";
         break;
      case REASON_RECOMPILE:
         text="Program "+__FILE__+" was recompiled";
         break;
      case REASON_REMOVE:
         text="Program "+__FILE__+" was removed from chart";
         break;
      case REASON_TEMPLATE:
         text="New template was applied to chart";
         break;
      default:
         text="Another reason";
     }
//---
   return text;
  }
//+------------------------------------------------------------------+
