# Written for Python 3.9.7, likely compatible with other python3 versions

# requests is used to query the Forced Photometry Service
import requests

# bs4 (Beauiful Soup 4) is used to parse the html output of request's methods
from bs4 import BeautifulSoup

# Python packages
from time import sleep
import pandas as pd
# IPAC Auth (Don't change these)
IUN = 'ztffps'
IPW = 'dontgocrazy!'

# (USER DETERMINED)

ralist=[]
declist=[]

Email = '...'
UserPass = '...'
ReqIDl = [0]*len(ralist)
filel = [0]*len(ralist)
dl = [False]*len(ralist)
firstReqID=0000000
RequestDF = pd.DataFrame(data={'ReqRA':ralist,'ReqDec':declist,'ReqID':ReqID,'file':file,'downloaded':dl})

# URL for checking the status of jobs sent by user
# Note: This webpage only updates once an hour, on the hour
StatusURL = 'https://ztfweb.ipac.caltech.edu/cgi-bin/getForcedPhotometryRequests.cgi'

    
# Open loop to periodically check the Forced Photometry job status page 
    
while(True):

            
        
    # Query service for jobs sent in past 30 days
    OutputStatus = requests.get(StatusURL, auth=(IUN, IPW), params={'email': Email, 'userpass': UserPass, 'option': 'All recent jobs', 'action': 'Query Database'})

    # Check if job has ended and lightcurve file was created
    # Note: If an exotic error occurs and the ended field is not populated, this will go on forever
    # Table has 11 cols, reqid=0, ended=7, lc=10

    # Format HTML
    OutputSoup = BeautifulSoup(OutputStatus.text, 'html.parser')
    # Get Job information in HTML table
    OutputTable = OutputSoup.find('table')
    OutputEnded=''
        
    # Verify Table exists (If no requests have been sent in past 30 days or if service is updating, this will catch it)
    if OutputTable != '' and OutputTable is not None:
        # Parse Table rows 
        for j, rowR in RequestDF.iterrows():
            for row in OutputTable.find_all('tr'):
                # Parse Table entries
                cols=row.find_all('td')
                if len(cols) > 0:
                    # Check if values contained in a given row coorespond to the current request
                    # Use a nested for loop here to check all requests submitted if there are more than one pending
                    OutputID=int(cols[0].text.strip())
                    OutputRA=float(cols[1].text.strip())
                    OutputDec=float(cols[2].text.strip())
                    OutputJDS=float(cols[3].text.strip())
                    OutputJDE=float(cols[4].text.strip())
                    OutputEnded=cols[7].text.strip()
                    # Check if job is finished (OutputEnded)
                    # Check for equality between recorded request params and what is known to be in the Job Status table, accounting for rounding
                    if(abs(OutputRA-rowR['ReqRA'])<=0.00001 and abs(OutputDec-rowR['ReqDec'])<=0.00001 and OutputID>=firstReqID):
                        
                        RequestDF['ReqID'][i] = OutputID
                            
                        if(OutputEnded != ''):
                            # Get end time of job and lightcurve path
                            OutputLC=cols[10].text.strip()
                            if(rowR['file'] == ''):
                                RequestDF['file'][i] = OutputLC
                                ForcedPhotometryReqURL = f'https://ztfweb.ipac.caltech.edu{OutputLC}'
                                LocalFilename = f'./{ForcedPhotometryReqURL.split("/")[-1]}'
                                with requests.get(ForcedPhotometryReqURL, stream=True, auth=(IUN, IPW)) as r:
                                # Write to the local file in chunks in case file is large
                                    with open (LocalFilename, 'wb') as f:
                                        for chunk in r.iter_content(chunk_size=8192):
                                            f.write(chunk)
                                if(os.path.exists(LocalFilename)):
                                        RequestDF['downloaded'][i] = True
                                        print(f'downloaded finished req: {OutputRA}, {OutputDec}')
                                        
                                OutputEnded=cols[7].text.strip()
                                # Set open loop hooks 
                                
      
            
       
            
       
RequestDF.to_csv("./fphot_reqstatus.csv",ignore_index=False)