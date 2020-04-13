#Author: Spencer Healy 
import time
import mysql.connector
from mysql.connector import errorcode
import shutil
import os
import glob
import os.path

#where large images end up 
source_dir = "C:\Users\skeel\Documents\FreelanceWork\MYSQLImage\pythonPractice"
#where you prefer them to be moved
dst = 'C:\Users\skeel\Documents\FreelanceWork\MYSQLImage\pythonPractice\largeSize'

#Running totals of failed and successful image conversions
totalSuccessLarge=0
totalFailLarge=0
totalSuccessThumb=0
totalFailThumb=0

print("WELCOME TO SUPER MEGA AWESOME IMAGE CONVERTER (DELUXE)!")

#Create list that will hold variable values from config file
configVariables = []
#Read file line by line into variables
filepath = 'imgConvert-Config.txt'  
f = open(filepath)
line = f.readline()
while line:
	#print(line)
	configVariables.append(line)
	# use realine() to read next line
	line = f.readline()
f.close()

#print(len(configVariables))
#print "List contents"
#for x in configVariables:
#	print(x)

print("Loading config file...")
#pw="fishing"
pw="Thirdeye69!"
database=configVariables[0]
user=configVariables[1]
host=configVariables[2]
pathFull=configVariables[3]
#print("Path full =",pathFull)
pathSmall=configVariables[4]

time.sleep(1)

print("Connecting to DB...")

time.sleep(2)

try:
        #connect to the db
	cnx = mysql.connector.connect(user=user, password=pw, host=host, database=database)

        #declare a list that will hold part id's, so we can skip ones that were already processed.
	#This came about from a bug that was overwriting the actual image we wanted with the incorrect image.
	partIdList=[]
	#print("Initial Size of partIdList:", len(partIdList))
	
	print("executing queries...")

	#Query 1:images
	sql_select_Query = "select * from image LIMIT 10"
	#sql_select_Query = "select * from image where RecordID > 16067 and RecordID < 16069"  #avoid the bad record:  left side

	#These 2 image ids should trigger the image overwriting bug
	#sql_select_Query = "select * from image where id = 1867 or id = 1241"
	#additional ids supposedly not converting the right image after 1st fix
	#sql_select_Query = "select * from image where id = 2264 or id = 2266"
	
	cursor = cnx.cursor()
	cursor.execute(sql_select_Query)
	records = cursor.fetchall()

	print("Total number of rows in image is - ", cursor.rowcount)
	print("Printing each row's column values i.e.  image record")
	for row in records:
                #check if the part id already exists in the list
                if row[3] in partIdList:
                        print("This part id has been processed already! Skipping this iteration...")
                else:
                        #print("Part id not in list yet...Adding part id to list, and proceeding with image extraction.")
                        partIdList.append(row[3])
                        
                        print("==================================")
                        print("Image Id == ", row[0])
                        #print("Full Size image = ", row[1])                    #SDZ i turned off all that scrolling text
                        #print("Thumbnail image  = ", row[2])
                        print("Part ID = ", row[3], "\n")
                                
                        print("Current total of duplicates found:", len(partIdList))
                        print("IDS that were marked as duplicates:")
                        print(partIdList)
                        
                        time.sleep(2)
                        #Name each image product.num-part.num.jpg
                        #The image recordID is coorelated to the id in both product and part
                        #Query 2: Product num
                        # sql_select_Query2 = "select num from product where id = %s"
                        #		imgId = (row[3],)#must be a tuple, so include the comma
                        imgId = (row[3],)#must be a tuple, so include the comma

                        # cursor.execute(sql_select_Query2, imgId)
                        # records2 = cursor.fetchall()
                        # for row2 in records2:
                        # 	#prodNum = row2[0]
                        # 	#print ("Product number for this image = ", prodNum)
                        # 	#finalImageName = pathFull + prodNum
                        # 	#print ("Final image name (where large images are saved) = ", finalImageName)
                        # 	#time.sleep(10)
                        # 	print("Not concerned with product number...")

                        #Query3 : Part num
                        sql_select_Query3 = "select num from part where id = %s"
                        #sql_select_Query3 = "select distinct num from part where id = %s limit 1"

                        #changing query temporarily to trigger image overwriting bug
                        #sql_select_Query3 = "select num from part where id = 1867 or id = 1241"
                        
                        #cursor.execute(sql_select_Query3, imgId)
                        cursor.execute(sql_select_Query3, imgId)
                        records3 = cursor.fetchall()
                        #records3 = cursor.fetchone()

                        # i need to convert this to be a single loop

                        for row3 in records3:
                                partNum = row3[0]
                                print("In the picture loop")
                                print("Image ID = ", imgId)
                                print("Part number for this image = ", partNum)
                                finalImageName = ''.join((partNum, ".png"))#converts the tuples to strings
                                print("Combined part num with .png = ",finalImageName)
                                #finalImageName2 = pathSmall + finalImageName #the path where to save the thumbnail images
                                #print ("Final image name 2 (where small images are saved) = ", finalImageName2)
                                time.sleep(3)
                                #finalImageName3 = pathFull + finalImageName

                        #some images fail due to bad data in the db (specifically product num), so continue the process even if an image fails
                        try:
                                #Convert image string to its png form
                                print("Part id not in list yet...Adding part id to list, and proceeding with image extraction.")
                                print("Converting full size image...")
                                str = row[1]#full size image
                                fh = open(finalImageName, "wb")
                                fh.write(str.decode('base64'))
                                fh.close()
                                totalSuccessLarge+=1
                        except:
                                print("This large image failed...skipping and continuing process...")
                                totalFailLarge+=1
                                time.sleep(5)
                                pass
		
		#end of 1st for loop-----
		
		#try:
                        # We dont need the Thumbs so steven commented out
			#print("Converting thumbnail size image...")
			#str = row[2]#thumb image
			#fh = open(finalImageName2, "wb")
			#fh.write(str.decode('base64'))
			#fh.close()
			#totalSuccessThumb+=1
##		except:
##			print("This thumbnail image failed...skipping and continuing process...")
##			totalFailThumb+=1
##			time.sleep(1)
##			pass

		# print("Converting thumbnail size image...")
		# str = row[2]#thumb image
		# fh = open(finalImageName2, "wb")
		# fh.write(str.decode('base64'))
		# fh.close()
		#cursor.close()

except mysql.connector.Error as err:
  if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
    print("Something is wrong with your user name or password")
  elif err.errno == errorcode.ER_BAD_DB_ERROR:
    print("Database does not exist")
  else:
    print(err)
else:
  cnx.close()

print("Moving large images into their folder...")
#move all large images into its folder
try:
	files = glob.iglob(os.path.join(source_dir, "*.png"))
	for file in files:
		if os.path.isfile(file):
			shutil.move(file, dst)
except:
	print("File already exists, skipping and continuing operations")
	pass

# files = glob.iglob(os.path.join(source_dir, "*.png"))
# for file in files:
# 	if os.path.isfile(file):
# 		shutil.move(file, dst)

#delete all png files in root folder (these are duplicates)
filelist = glob.glob(os.path.join(source_dir, "*.png"))
for f in filelist:
    os.remove(f)

print("+++++++++++++++++++++++++++++++++++++++++++++++++")
print("PROCESS COMPLETE!")
print("Only the 1st image is converted if 2 of the same part id were found.")
print ("Large Image Success total = ", totalSuccessLarge)
print ("Large Image Failed Total = ", totalFailLarge)
print ("Number of duplicates skipped = ", len(partIdList))
#print ("Thumb Image Success total = ", totalSuccessThumb)
#print ("Thumb Image Failed Total = ", totalFailThumb)

time.sleep(100)
