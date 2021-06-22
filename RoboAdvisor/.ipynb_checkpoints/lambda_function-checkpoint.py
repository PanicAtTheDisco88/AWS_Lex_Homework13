### Required Libraries ###
import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from botocore.vendored import requests
import alpaca_trade_api as tradeapi
from MCForecastTools import MCSimulation
import pandas as pd
import json

### Functionality Helper Functions ###
def parse_float(n):
    """
    Converts string to float.
    """
    try:
        return float(n)
    except ValueError:
        return float("nan")
    
def get_portfolio_df():
    """
    Gets closing prices for SPY and AGG from Alpaca and returns dataframe
    """
    SPY_daily_url = config.BARS_URL + '/1D?symbols=SPY&limit=252'
    SPY_data = requests.get(SPY_daily_url, headers=config.HEADERS).json()
    AGG_daily_url = config.BARS_URL + '/1D?symbols=AGG&limit=252'
    AGG_data = requests.get(AGG_daily_url, headers=config.HEADERS).json()

    SPY_date_list = []
    SPY_close_list = []
    AGG_date_list = []
    AGG_close_list = []
    
    for x in range(len(SPY_data['SPY'])):
        t = SPY_data['SPY'][x]['t']
        SPY_date_list.append(datetime.fromtimestamp(t).isoformat())
        SPY_close_list.append(SPY_data['SPY'][x]['c'])

    df1 = pd.DataFrame({'time':SPY_date_list, 'close':SPY_close_list})
    
    for x in range(len(AGG_data['AGG'])):
        t = AGG_data['AGG'][x]['t']
        AGG_date_list.append(datetime.fromtimestamp(t).isoformat())
        AGG_close_list.append(AGG_data['AGG'][x]['c'])
        
    df2 = pd.DataFrame({'time':AGG_date_list, 'close':AGG_close_list})
    df2 = df2.drop(['time'], axis=1)
    portfolio_df = pd.concat([df1, df2], axis="columns", join="inner")
    portfolio_df.set_index(portfolio_df.time, inplace=True)
    portfolio_df = portfolio_df.drop(['time'], axis=1)
    portfolio_df.columns = pd.MultiIndex.from_arrays([portfolio_df.columns, ['SPY','AGG']])
    portfolio_df.columns = portfolio_df.columns.swaplevel(0, 1)

    return portfolio_df

def build_validtion_result(is_valid, violated_slot, message_content):
    """
    Defines an internal validation message structured as a python dictionary.
    """
    if message_content is None:
        return {"isValid": is_valid, "violatedSlot": violated_slot}
    return {
        "isValid": is_valid,
        "violatedSlot": violated_slot,
        "message": {"contentType": "PlainText", "content": message_content}
    }

def validate_data(age, investment_amount):
    """
    Validates the data provided by the user.
    """
    # Validate the user is over 18 years old
    if age is not None:
        age = parse_float(age)
        if age < 18:
            return build_validtion_result(False, "age", "You must be over 18 to use this service.")
    
    # Validate the amount to invest is greater than zero
    if investment_amount is not None:
        investment_amount = parse_float(investment_amount)
        if investment_amount <= 0:
            return build_validtion_result(False, "investment_amount", "The amount to invest should be greater than zero. Please provide the amount in USD to invest.")
    
    # Return a true result if age and investment_amount are valid
    return build_validtion_result(True, None, None)

### Dialogue Actions Helper Functions ###
def get_slots(intent_request):
    """
    Validates the data provided by the user.
    """
    return intent_request["currentIntent"]["slots"]

def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    """
    Defines an elicit slot type response
    """
    return{
        "sessionAttributes": session_attributes,
        "dialogAction":{
            "type": "ElicitSlot",
            "intentName": intent_name,
            "slots": slots,
            "slotToElicit": slot_to_elicit,
            "message": message,
        },
    }

def delegate(session_attributes, slots):
    """
    Defines a delegate slot type response
    """
    return{
        "sessionAttributes": session_attributes,
        "dialogAction":{
            "type": "Delegate",
            "slots": slots
        },
    }

def close(session_attributes, fulfillment_state, message):
    """
    Defines a close slot type response
    """
    response = {
        "sessionAttribute": session_attributes,
        "dialogAction": {
            "type": "Close",
            "fulfillmentState": fulfillment_state,
            "message": message,
        },
    }
    return response

def recommend_portfolio(risk_level):
    """
    Performs dialog management and fulfillment for recommending a portfolio.
    """
        
    if risk_level == 'none':
        #100% bonds (AGG), 0% equities (SPY)
        weights = [1,0]
    elif risk_level == 'veryLow':
        #80% bonds (AGG), 20% equities (SPY)
        weights = [.8,.2]
    elif risk_level == 'low':
        #60% bonds (AGG), 40% equities (SPY)
        weights = [.6,.4]
    elif risk_level == 'medium':
        #40% bonds (AGG), 60% equities (SPY)
        weights = [.4,.6]
    elif risk_level == 'high':
        #20% bonds (AGG), 80% equities (SPY)
        weights = [.2,.8]
    else:
        #0% bonds (AGG), 100% equities (SPY)
        weights = [0,1]
    
    # Configure a Monte Carlo simulation to forecast 10 year returns on portfolio of AGG & SPY
    # Set number of simulations
    num_sims = 100
    # Set number of years to simulate
    sim_years = 10
    portfolio_df = get_portfolio_df()

    MC_portfolio = MCSimulation(
        portfolio_data = get_portfolio_df(),
        weights = weights,
        num_simulation = num_sims,
        num_trading_days = 252*sim_years
    )
    
    tbl = MC_portfolio.summarize_cumulative_return()
    ci_lower = round(tbl[8],2)
    ci_upper = round(tbl[9],2)

    initial_recommendation = print(f'{first_name}, the 95% confidence interval for {sim_years} year returns on a {intent_request[3]} risk portfolio is {ci_lower}x - {ci_upper}x\n')

    return initial_recommendation

### Intent Handler ###
def portfolio_advice(intent_request):
    """
    Performs dialog management and fulfillment for recommending a portfolio.
    """    
    # Gets slots' values
    first_name = get_slots(intent_request)["firstName"]
    age = get_slots(intent_request)["age"]
    investment_amount = get_slots(intent_request)["investmentAmount"]
    risk_level = get_slots(intent_request)["riskLevel"]
    
    # Gets the invocation source, for Lex dialogs "DialogCodeHook" is expected.
    source = intent_request["invocationSource"]

    if source == "DialogCodeHook":
        # Perform basic validation on the supplied input slots.
        
        ### DATA VALIDATION CODE STARTS HERE ###
        # This code performs basic validation on the supplied input slots.

        # Gets all the slots
        slots = get_slots(intent_request)
       

        # Validates user's input using the validate_data function
        validation_result = validate_data(age, investment_amount)

        # If the data provided by the user is not valid, the elicitSlot dialog action is used to re-prompt for the first violation detected.
        
        if not validation_result["isValid"]:
            slots[validation_result["violatedSlot"]] = None  # Cleans invalid slot

            # Returns an elicitSlot dialog to request new data for the invalid slot
            return elicit_slot(
                intent_request["sessionAttributes"],
                intent_request["currentIntent"]["name"],
                slots,
                validation_result["violatedSlot"],
                validation_result["message"],
            )

        # Fetch current session attributes
        output_session_attributes = intent_request["sessionAttributes"]

        # Once all slots are valid, a delegate dialog is returned to Lex to choose the next course of action.
        return delegate(output_session_attributes, get_slots(intent_request))
        
        ### VALIDATION CODE ENDS HERE ###
        

    # Get the initial investment recommendation

    ### YOUR FINAL INVESTMENT RECOMMENDATION CODE STARTS HERE ###
    recommend_portfolio(risk_level)

    ### YOUR FINAL INVESTMENT RECOMMENDATION CODE ENDS HERE ###

    # Return a message with the initial recommendation based on the risk level.
    return close(
        intent_request["sessionAttributes"],
        "Fulfilled",
        {
            "contentType": "PlainText",
            "content": """{}, thank you for opportunity to inform your investment decisions.""".format(first_name),
        },
    )

### Intents Dispatcher ###

def dispatch(intent_request):
    """
    Called when the user specifies an intent for this bot.
    """

    # Get the name of the current intent
    intent_name = intent_request["currentIntent"]["name"]
    
    # Dispatch to bot's intent handlers
    if intent_name == "RecommendPortfolio":
        return portfolio_advice(intent_request)

    #raise Exception("Intent with name " + intent_name + " not supported")

### Main Handler ###
def lambda_handler(event, context):
    """
    Route the incoming request based on intent.
    The JSON body of the request is provided in the event slot.
    """

    return dispatch(event)
