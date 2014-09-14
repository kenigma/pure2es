import sys
import pytz
from datetime import datetime, timedelta
from pytz import timezone
import json
from elasticsearch import Elasticsearch

def composeESBulkIndexBody(attnList, index, index_type):
  temp = {
    'index' : {
      '_index' : index,
      '_type' : index_type
    }
  }
  lines = []
  for a in attnList:
    actLine = temp.copy()
    actLine['index']['_id'] = a['id']
    lines.append(json.dumps(actLine))
    temp_a = a.copy()
    temp_a['datetime'] = a['datetime'].isoformat()
    lines.append(json.dumps(temp_a))
  
  return "\n".join(lines)

def readFileAsList(filename, yogi, tz):
  attnList = []
  f = open(filename, 'rU')
  lines = f.read().split("\n")
  print "Skipping the following header row:"
  print lines.pop(0)  #remove the header row
  print "Processing", len(lines), "row..."
  for line in lines:
    tokens = line.split("\t")
    if len(tokens) != 11: 
      print "Error: token size != 10"
      print "Skipping row >>>", tokens
      continue
#7/09/2014	Sunday	9:30 AM	Anri Shiga	Yoga - Lincoln House	HK All Clubs - 12 Mths Unlimited (Yoga Access)	Hatha 1	Signed in	Yes	103337894	
#6/09/2014	Saturday	11:16:14 AM	STAFF STAFF	Fitness - Kinwick Centre	HK All Clubs - 12 Mths Unlimited (Fit Access)	Arrival	Signed in	No	103337893	
#5/09/2014	Friday	6:15 PM	Anri Shiga	Yoga - Lincoln House	HK All Clubs - 12 Mths Unlimited (Yoga Access)	Vinyasa Flow	Signed in	Yes	103337894	
#4/09/2014	Thursday	7:30 PM	Paris Wong	Yoga - Lincoln House	HK All Clubs - 12 Mths Unlimited (Yoga Access)	Hatha 1	Signed in	Yes	103337894	

    
    #print "Parsing:", tokens[0], tokens[2]
    dtStr = tokens[0] + " " + tokens[2]
    try:
      dt = tz.localize(datetime.strptime(dtStr, "%d/%m/%Y %I:%M %p"))
    except ValueError:
      dt = tz.localize(datetime.strptime(dtStr, "%d/%m/%Y %I:%M:%S %p"))
    
    rec = {
      'id' : yogi + "_" + dt.strftime("%Y%m%d%H%M%S"),
      'datetime'  : dt,
      'teacher'   : tokens[3],
      'teacher_raw' : tokens[3],
      'location' : tokens[4],
      'location_raw' : tokens[4],
      'class_type' : tokens[6],
      'class_type_raw' : tokens[6],
      'status' : tokens[7],
      'yogi' : yogi
    }
    
    attnList.append(rec)
    
  
  
  f.close()
  return attnList

###

# This basic command line argument parsing code is provided and
# calls the print_words() and print_top() functions which you must define.
def main():
  if len(sys.argv) != 6:
    print 'usage: ./import.py <raw file> <yogi name> <esHost> <esIndex> <esType>'
    sys.exit(1)

  filename = sys.argv[1]
  yogi = sys.argv[2]
  esHost = sys.argv[3]
  esIndex = sys.argv[4]
  esType = sys.argv[5]
  
  tz = timezone('Asia/Hong_Kong')
 
  attnList = readFileAsList(filename, yogi, tz)
  body = composeESBulkIndexBody(attnList, esIndex, esType)
  
  # connect to ES
  es = Elasticsearch([esHost])
  es.bulk(body=body, index=esHost, doc_type=esType)
  
  #print "List size:", len(attnList)
  #print attnList
  #print body
  
if __name__ == '__main__':
  main()