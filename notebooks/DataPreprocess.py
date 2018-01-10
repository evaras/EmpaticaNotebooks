print("Importing libraries")
import pandas as pd
from pandas import Series, DataFrame
import datetime as dt
import scipy as sci
from scipy import signal
from scipy.interpolate import UnivariateSpline
import csv
import datetime
import math
import time
import collections
from collections import OrderedDict
import os.path
import argparse
print("Libaries imported")

path = os.path.abspath(__file__).split('/')
path.pop()
path = '/'.join(path)

devnull = open(os.devnull, 'w')


def main():
	print("Reading arguments")

	parser = argparse.ArgumentParser(description='Data preprocess for model')
	#Arguments needed for the script to work with the parameters obtained in the session
	parser.add_argument('PATH', help='Path to the downloaded data', type=str)
	parser.add_argument('SCORE', help='Score obtained in the session being imported', type=float)
	parser.add_argument('TRIES', help='Number of tries in the session being imported', type=float)
	parser.add_argument('VALENCIA', help='Valencia value obtained in the session quiz', type=float)
	parser.add_argument('ACTIVATION', help='Activation obtained in the session quiz', type=float)
	parser.add_argument('BASE', help='Time in minutes measured as base before starting the session activity in minutes', type=int)

	args = parser.parse_args()

	#Initial function for the data load
	load(args)
	#Filtering the original data for smoothing the signal. Also helps the visualization if needed.
	print("Filtering data")
	filter()
	#Function for maximums location
	print("Maximums calculation")
	maxs()
	#Interesting stadisitics finding
	print("Satatistics obtention")
	stds()
	#Appending and saving the data in the study csv
	print("Saving data")
	save()

def joinCSVempatica(args):
	global gravX
	global gravY
	global gravZ  
	gravX = 0
	gravY = 0
	gravZ = 0

	global EDAHertz
	global BVPHertz
	global TEMPHertz
	global ACCHertz
	EDAHertz = 4
	BVPHertz = 64
	TEMPHertz = 4
	ACCHertz = 32

	#myDir = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))+"/" #Setting current dir
	#myDir = os.path.split(os.path.abspath(myDir))[0]

	myDir = dir_path = args.PATH+'/'
	participantID = os.path.split(os.path.abspath(myDir))[1] #Parent of parent directory as ParticipantID for the case of aquarium di Genova data

	outputFile = args.PATH+"/mergedBioData.csv" #Setting name of output file

	print("Current Directory: ",myDir)
	print("Syncing data for Participant: ",participantID)



	ACC = {}
	ACC = readAccFile(myDir+'ACC.csv')

	HR = {}
	HR = readFile(myDir+'HR.csv')

	EDA = {}
	EDA = readFile(myDir+'EDA.csv')

	BVP = {}
	BVP = readFile(myDir+'BVP.csv')

	TEMP = {}
	TEMP = readFile(myDir+'TEMP.csv')

	IBI = {}
	IBI = readIBI_File(myDir+'IBI.csv')

	#merging all files at a sampling rate of 1 Hz
	count = 0 #count of how many timestamps are the same
	total = 0 #total of measurements with the same timestamp

	start_time = convertMilisToTime(time.time()+3600*2)

	with open(outputFile,'w') as f1:
	    writer=csv.writer(f1, delimiter=',',lineterminator='\n',)
	    row ="ID","Timestamp","Hour","HRV","EDA","BVP","TEMP","ACC_X","ACC_Y","ACC_Z","ACC_Overall","SumIBI","Beats"
	    writer.writerow(row)
	    for timestampHR, hr in HR.items():
	        timestamp = convertMilisToTime(timestampHR)
	        hour = timestamp.split(" ")#splitting timestamp and keeping hour for importing to SPSS
	        #merging with EDA
	        i = 0.0
	        total = 0.0
	        count = 0
	        meanEDA = 0.0
	        while i < 1.0:
	            if (timestampHR + i in EDA):
	                total = total + float(EDA[timestampHR+i])
	                count = count+1
	            i = i + 1.0/EDAHertz
	        if(count > 0):
	            meanEDA = total/count
	        print("Merging HRV and EDA at ", timestamp, " HRV: ",hr," EDA ",meanEDA, " count: ",count)
	        #merging with BVP
	        i = 0.0
	        total = 0.0
	        count = 0
	        meanBVP = 0.0
	        while i < 1.0:
	            if (timestampHR + i in BVP):
	                total = total + float(BVP[timestampHR+i])
	                count = count+1
	            i = i + 1.0/BVPHertz
	        if(count > 0):
	            meanBVP = total/count
	        print("Merging HRV and BVP at ", timestamp, " HRV: ",hr," BVP ",meanBVP, " count: ",count)
	        #merging with TEMP
	        i = 0.0
	        total = 0.0
	        count = 0
	        meanTemp = 0.0
	        while i < 1.0:
	            if (timestampHR + i in TEMP):
	                total = total + float(TEMP[timestampHR+i])
	                count = count+1
	            i = i + 1.0/TEMPHertz
	        if(count > 0):
	            meanTEMP = total/count
	        print("Merging HRV and TEM at ", timestamp, " HRV: ",hr," TEM ",meanTEMP, " count: ",count)
	        #merging with ACC
	        i = 0.0
	        totalX = 0.0
	        totalY = 0.0
	        totalZ = 0.0
	        totalOverall = 0.0
	        count = 0
	        meanX = 0.0
	        meanY = 0.0
	        meanZ = 0.0
	        meanOverall = 0.0
	        while i < 1.0:
	            if (timestampHR + i in ACC):
	                totalX = totalX + float(ACC[timestampHR+i]['x'])
	                totalY = totalY + float(ACC[timestampHR+i]['y'])
	                totalZ = totalZ + float(ACC[timestampHR+i]['z'])
	                totalOverall = totalOverall + float(ACC[timestampHR+i]['overall'])
	                count = count+1
	            i = i + 1.0/ACCHertz
	        if(count > 0):
	            meanX = totalX/count
	            meanY = totalY/count
	            meanZ = totalZ/count
	            meanOverall = totalOverall/count
	        print("Merging HRV and ACC at ", timestamp, " HRV: ",hr," ACC ",meanOverall, " count: ",count)
	        #merging with IBI in 1 second timeframes: Sums up all IBI occurring in 1 sec time frame.
	        i = 0.0
	        total = 0.0
	        count = 0
	        sumIBI = 0.0
	        while i < 1.0:
	            if(timestampHR + i in IBI):
	                #print "Timestamps matched-- HR:",timestampHR," IBI: ",timestampHR+i
	                total = total + float(IBI[timestampHR+i])
	                count = count + 1
	            i = i + 0.1
	        if(count > 0):
	            sumIBI = total
	        print("Merging HRV and IBI at ", timestamp," milis: ",timestampHR," HRV: ",hr, " Sum IBI: ",sumIBI, " count: ",count)

	        row = participantID,timestamp,hour[1],hr,meanEDA,meanBVP,meanTEMP,meanX,meanY,meanZ,meanOverall,sumIBI,count
	        writer.writerow(row)
	print("--------------------------------------------------------------------------------")
	print("Synced data for Participant: ",participantID)
	print("Start time: ", start_time, " End time: ", convertMilisToTime(time.time()+3600*2))
	print("Results stored in ",outputFile)

def load(args):
	joinCSVempatica(args)
	global df 
	df = pd.read_csv(args.PATH+"/mergedBioData.csv", sep=',', header=0)
	global score 
	score = args.SCORE
	global tries 
	tries = args.TRIES
	global valencia
	valencia = args.VALENCIA
	global activation
	activation = args.ACTIVATION
	global base
	base = args.BASE
	df['Timestamp'] = df['Timestamp'].apply(lambda x: dt.datetime.strptime(x,'%Y-%m-%d %H:%M:%S'))

def filter():
	#Filter applied (Savitzki Golay, window size 21, polynomial order 3)
	print(df)
	yhat = sci.signal.savgol_filter(df["EDA"], 21, 3) 
	df["FilterEDA"]= yhat

	#Second filter helpful for activity separation (Savitzki Golay, window size 501, polynomial order 3)
	yhat = sci.signal.savgol_filter(df["FilterEDA"], 501, 3) 
	df["NEDA"]= yhat

def maxs():
	#Maximums mapping
	wsize = 15                                               #Window size
	maximuns = df.rolling(wsize)["FilterEDA"].max()          #maximum value per window
	maximuns.dropna(0,inplace=True)                          #NaN delete

	maxcount = maximuns.value_counts()                       #Number of windows with a value as max
	df_maxs=pd.DataFrame(maxcount)                           #Dataframe creation              
	df_maxs = df_maxs.where(df_maxs['FilterEDA'] >wsize-1)   #Removal of the fields which not match the condition
	df_maxs.dropna(0,inplace=True)                           #NaN delete

	df["ismax"]= False                                       #Field ismax is created in the original dataframe

	maxs=df_maxs.index                                       #List with the maxs values
	df_max = pd.DataFrame()                                  #New DF
	df_max["values"]= maxs                                   #DF with organized maxs

	for index, row in df_max.iterrows():                    #Iteration for changing the ismax field in the maxs
	    df.ix[df.FilterEDA == row['values'], 'ismax']=True

def stds():
	#Ineresting Statistics obtention
	std = df.describe()
	std = std.drop(['ACC_X','ACC_Y', 'ACC_Z', 'ACC_Overall', 'EDA', 'SumIBI', 'Beats', 'NEDA' ], 1)
	std = std.drop(['count','25%', '50%', '75%', 'max', 'min'], 0)

	#Shannon entropy obtention
	hrv=sci.stats.entropy(df["HRV"].astype(int))
	eda=sci.stats.entropy(df["FilterEDA"].astype(int))
	bvp=sci.stats.entropy(df["BVP"].astype(int))
	temp=sci.stats.entropy(df["TEMP"].astype(int))
	s = pd.Series([hrv,bvp,temp,eda],index=['HRV','BVP','TEMP', 'FilterEDA'], name="ShEn")
	std = std.append(s)

	#Initial vs session statistics obtention
	tbase = base*60                 #Time resting before session
	df_base = df.iloc[0:tbase]  	#base time data obtention
	mBase = df_base.mean()      
	mBase = mBase.drop(['ACC_X','ACC_Y', 'ACC_Z', 'ACC_Overall', 'EDA', 'SumIBI', 'Beats', 'NEDA', 'ismax' ])
	mMedida = df.mean()         
	mMedida = mMedida.drop(['ACC_X','ACC_Y', 'ACC_Z', 'ACC_Overall', 'EDA', 'SumIBI', 'Beats', 'NEDA', 'ismax' ])
	mRel = mBase/mMedida        
	mRel =  pd.Series(mRel,index=['HRV','BVP','TEMP', 'FilterEDA'], name="meanRelation")
	mBase =  pd.Series(mBase,index=['HRV','BVP','TEMP', 'FilterEDA'], name="meanBase")
	std = std.append(mBase)
	std = std.append(std.iloc[0])
	std = std.append(mRel)
	uno = std["HRV"].iloc[0]
	dos = std["HRV"].iloc[1]
	tres = std["BVP"].iloc[0]
	cuatro = std["BVP"].iloc[1]
	cinco = std["TEMP"].iloc[0]
	seis = std["TEMP"].iloc[1]
	siete = std["FilterEDA"].iloc[3]
	ocho = std["FilterEDA"].iloc[4]
	nueve = std["FilterEDA"].iloc[0]
	diez = std["FilterEDA"].iloc[1]
	once = std["FilterEDA"].iloc[5]
	doce = score
	trece = tries
	catorce = valencia
	quince = activation
	global info 
	info = pd.Series([uno,dos,tres,cuatro,cinco,seis,siete,ocho,nueve,diez,once,doce,trece,catorce,quince],index=["HRV mean", "HRV std", "BVP mean", "BVP std", "TEMP mean", "TEMP std", "EDA mean(base)", "EDA std(base)","EDA mean(session)", "EDA std(session)", "EDA mean relation", "Score", "Fails", "Valencia", "Activation" ])

def save():
	data = pd.read_csv("data.csv", sep=',', index_col=0)
	data = data.append(info, ignore_index=True)
	data.to_csv("data.csv")
	print(data)

def convertMilisToTime(milis):
    return time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(round(milis))))

def processAcceleration(x,y,z):
    #converting to G values: https://support.empatica.com/hc/en-us/articles/201608896-Data-export-and-formatting-from-Empatica-Connect-
    x = float(x) * 2/128
    y = float(y) * 2/128
    z = float(z) * 2/128
    #calculating effect of gravity
    alpha = 0.8
    global gravX
    global gravY
    global gravZ
    #Global variables for applying low pass filter on acceleration values
    gravX = alpha * gravX + (1 - alpha) * x;
    gravY = alpha * gravY + (1 - alpha) * y;
    gravZ = alpha * gravZ + (1 - alpha) * z;
    #removing gravity's effect: https://developer.android.com/reference/android/hardware/SensorEvent.html#values
    x = x - gravX
    y = y - gravY
    z = z - gravZ
    #total acceleration from all 3 axes: http://physics.stackexchange.com/questions/41653/how-do-i-get-the-total-acceleration-from-3-axes
    overall = math.sqrt(x*x+y*y+z*z)
    return {'x':x,'y':y,'z':z,'overall':overall}

def readFile(file):
    dict = OrderedDict()
    print("-->Reading file:" + file)
    with open(file, 'rt') as csvfile:
         reader = csv.reader(csvfile, delimiter='\n')
         i=0;
         for row in reader:
             if(i == 0):
                 timestamp=row[0]
                 timestamp = float(timestamp)+3600*2#converts from string to float rounds and then to int
             elif(i == 1):
                 hertz=float(row[0])
             elif(i == 2):
                 dict[timestamp]=row[0]
                 #print ', '.join(row)
             else:
                 timestamp = timestamp + 1.0/hertz
                 #print timestamp
                 #print convertMilisToTime(timestamp)
                 dict[timestamp]=row[0]
             #print "HR Timestamp:",timestamp," exact Time: ",convertMilisToTime(timestamp)
             i = i + 1.0
    return dict

def readAccFile(file):
    dict = OrderedDict()
    print("-->Reading file:" + file)
    with open(file, 'rt') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        i=0;
        for row in reader:
            if(i == 0):
                timestamp = float(row[0])+3600*2#converts from string to float rounds and then to int
            elif(i == 1):
                hertz=float(row[0])
            elif(i == 2):
                dict[timestamp]= processAcceleration(row[0],row[1],row[2])
            else:
                timestamp = timestamp + 1.0/hertz
                dict[timestamp] = processAcceleration(row[0],row[1],row[2])
            i = i + 1

    return dict
#Reading IBI File
def readIBI_File(file):
    dict = OrderedDict()
    print("-->Reading file:" + file)
    with open(file, 'rt') as csvfile:
        reader = csv.reader(csvfile, delimiter = ',')
        initialTimestamp = 0.0
        i = 0;
        for row in reader:
            if(i == 0):
                initialTimestamp = float(row[0])+3600*2
                print("Initial Timestamp ",initialTimestamp," exact Time: ",convertMilisToTime(initialTimestamp))
            else:
                timestamp = initialTimestamp + round(float(row[0]),1)
                dict[timestamp] = float(row[1])
                #print "IBI timestamp",timestamp," exact Time: ",convertMilisToTime(timestamp)
            i = i + 1
    return dict

if __name__ == '__main__':
	main()