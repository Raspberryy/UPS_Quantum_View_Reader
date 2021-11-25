import xml.etree.ElementTree as ET
import requests as req
import os
import sys
import time
from qv_objects import *

# *******************************
# **    QV API Credentials     **
# *******************************

accessLicenseNumber =''
userId = ''
password = ''
customerContext = ''
subscriptionRequest = ''

# ****************************
# **        Settings        **
# ****************************

# SAVE_QV_RESPONSE_TO_FILE
# True  Saves QV API response to JSON file
# False Does not save QV API response to JSON file
SAVE_QV_RESPONSE_TO_FILE = True 

# SAVE_MERGED_QV_RESPONSE_ONLY
# True  If SAVE_QV_RESPONSE_TO_FILE is True, only the merged
#       QV API repsonse will be saved to JSON file. 
# False If SAVE_QV_RESPONSE_TO_FILE is True, saves all QV API
#       responses as well as the merged response to JSON file
SAVE_MERGED_QV_RESPONSE_ONLY = True


# *************************
# **    Query QV API     **
# *************************

def getURLHeaderJSON(_accessLicenseNumber, _userId, _password, _customerContext, _subscriptionRequest, _bookmark=None):
    """Returns URL header as string for QV API JSON request"""

    header = ('{ "AccessRequest": { "AccessLicenseNumber": "' + _accessLicenseNumber  
        + '", "UserId": "' + _userId + '", "Password": "' + _password 
        + '" }, "QuantumViewRequest": { "Request": { "TransactionReference": { "CustomerContext": "' + _customerContext 
        + '" }, "RequestAction": "QVEvents" }, "SubscriptionRequest": { "Name": "' + _subscriptionRequest + '" } }')
   
    if _bookmark is None:
        header += '}'
    else:
        header += ', "Bookmark":"' + _bookmark + '"}'
 
    return header 

def getQVData():
    """Gets available data from UPS QV API. Might send multiple requests if needed."""

    url = 'https://onlinetools.ups.com/rest/QVEvents'
    #url ='https://wwwcie.ups.com/rest/QVEvents' # DEV
    datetime = time.strftime("%Y%m%d_%H%M%S")
    bookmark = None
    finished = False
    data = []

    # Query API and add responses to data array
    while not finished:
        header = getURLHeaderJSON(accessLicenseNumber, userId, password, customerContext, subscriptionRequest, bookmark)
        request = req.post(url, header)
        data.append(request.json())

        # Check if there is more data
        bookmark = getValue(['QuantumViewResponse', 'Bookmark'], data[-1])
        if bookmark is None:
            finished = True

    # Merge responses
    data_length = len(data)
    for i in range(data_length):
        if i == 0:
            jsonObj = data[i]
        else:
            jsonObj["QuantumViewResponse"]["QuantumViewEvents"]["SubscriptionEvents"]["SubscriptionFile"].append(
                data[i]["QuantumViewResponse"]["QuantumViewEvents"]["SubscriptionEvents"]["SubscriptionFile"])

    # Save responses
    if SAVE_QV_RESPONSE_TO_FILE:
        with open(datetime + ".json" , "w") as outfile:
            json.dump(jsonObj, outfile)

        if SAVE_MERGED_QV_RESPONSE_ONLY is False and data_length > 1:
            # In addtion to merged repsonse, save sub responses too
            for i in range(data_length):
                with open(datetime + "_part_" + str(i+1) + ".json" , "w") as outfile:
                    json.dump(data[i], outfile)

    return jsonObj

				
# *************************
# **        MAIN         **
# *************************

def main():
    
    
    # Query QV API
    print(datetime.now(), "\tQuery QV API")
    jsonObj = getQVData()

    # Parse API response
    print(datetime.now(), "\tParsing response")
    shipments = parseShipments(jsonObj)
        
    # Store data in database
    print(datetime.now(), "\tSaving data to database")
    global_session.add_all(shipments)
    global_session.commit()

    # Clean Up
    print(datetime.now(), "\tFinished")

    return

if __name__ == '__main__':
    main()
