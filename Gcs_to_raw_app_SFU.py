import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
from sys import argv
from google.cloud import storage 
import datetime
import logging
from apache_beam import pvalue
import csv
from io import StringIO
import json
import sys
import os
import re
import yaml
# ********  Attention to development team *************************************************
#***************************** START *******************************************************
# variable/constant declarations.
# This section has to be modified based on GCP project and clients.


Project_Id ="tfo-eu-dev-synops-utilities"
Target_Dataset_Id='tfo_sfu_raw_dev' 
Target_table_id='Apparel_test' 
SOURCE_BUCKET_NAME='sfu-storage-dev' 
ERROR_TABLE= "tfo_sfu_raw_dev.tfo_sfu_raw_error_table" 
ERROR_TABLE_SCHEMA= "job_log_id:STRING,log_dttm:TIMESTAMP,failed_rcrd:STRING,error_msg:STRING,error_description:STRING" 
Error_log="sfu-storage-dev/Errorlogs" 
FileNot_Found_Error= "Errorlogs/SAXFileNotFoundError" 
Empty_FileError="Errorlogs/EmptySaxFileError" 
File_DataError="Errorlogs/SAXDataError" 
table_schema="Department_Number:NUMERIC,Credit_Office_Name:STRING,Market:STRING,PO_Issue_Date:DATE,PO_Number:NUMERIC,Country_of_Origin:STRING,Supplier_Name:STRING,Supplier_Number:NUMERIC,Buyer:STRING,Fineline_Number:NUMERIC,Quote_Id:NUMERIC,PO_Type:NUMERIC,PO_Qty_Units:NUMERIC,Prod_Start_Date:DATE,PO_Ship_Date_Original:DATE,PO_Ship_Date_Revised:DATE,PO_Cancel_Date:DATE,PO_Cancel_Date_Revised:DATE,Style_Vendor_Stock_Number:STRING,Product_Desc:STRING,Event_Code:STRING,Factory_Name:STRING,Loading_Port:STRING,Destination:STRING,In_Store_In_Storage_Date:DATE,MAAP:DATE,Brand:STRING,Season_Year:STRING,Shipping_Booking_Date:STRING,FCL_or_LCL:STRING,Cargo_Ready_Date:DATE,Final_Inspection_Date___Planned:DATE,Final_Inspection_Date___Actual:DATE,Final_Inspection_Result:STRING,Shipping_ETA:STRING,Shipping_ETD:STRING,Closing__Cut_Off_Date:DATE,Total_Factory_lines:FLOAT,Number_of_Lines_for_this_program:FLOAT,Average_Daily_Output_Per_Line:FLOAT,Sewn_Quantity_Planned:FLOAT,IsPOShipped:BOOLEAN,Sewn_Quantity_Actual:FLOAT,Washed_Quantity:FLOAT,Embellishment_Quantity:FLOAT,Packed_Quantity_Planned:FLOAT,Packed_Quantity_Actual:FLOAT,Sewn_Quantity_Status:STRING,Packed_Quantity_Status:STRING,Final_Inspection_Status:STRING,Shipping_Booking_Status:STRING,Cargo_Ready_Status:STRING,Vessel_Schedule_Status:STRING,Overall_Status:STRING,Carrier:STRING,Discharge_Port:STRING,OCM_Name:STRING,PO_Shipped_Date:DATE,First_Final_Inspection_Date:DATE,First_Final_Inspection_Result:STRING,Final_Inspection_Booking_Date:DATE,Final_Inspection_Date___Requested:DATE,Creation_Datetime:TIMESTAMP"
Target_Table_length='62'
    

PROCESSED_BUCKET_NAME="sfu-storage-dev" 


 
#***************************** END  *******************************************************
def validate_row(row):
    return True

    # count=0
    # if (row[3].isdigit()  or row[3] is None or len(row[3])==0 ):
        # count+=1
    # if (row[4].isdigit() or row[4] is None or len(row[4])==0 ):
        # count+=1
    # if (row[5].isdigit() or row[5] is None or len(row[5])==0 ):
        # count+=1
    # if (row[6].isdigit() or row[6] is None or len(row[6])==0 ):
        # count+=1 
    # if (row[7].isdigit() or row[7] is None or len(row[7])==0 ):
        # count+=1
    # if (row[0].isdigit() or row[0] is None or len(row[0])==0 ):
        # count+=1
    # if count==6:
        # return True
    # return False


class DataflowWalmart(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
      # Use add_value_provider_argument for arguments to be templatable
      # Use add_argument as usual for non-templatable arguments
    
      parser.add_value_provider_argument(
           '--GCS_File_location'
            ,default='gs://sfu-storage-dev/Apparel_Final_04242023.csv'
           ,help='Path of the file to read from')
      parser.add_argument(
           '--config_File'
            ,default='Gcs_to_raw_App_SFU.yml'
           ,help='Path of the file to read from')
      parser.add_argument(
           '--Project_id'
            ,default='tfo-eu-dev-synops-utilities'
           ,help='Path of the file to read from')
      
      parser.add_argument(
           '--ENV'
            ,default='tfo-eu-dev-synops-utilities'
           ,help='Path of the file to read from')
      parser.add_argument(
           '--SOURCE_BUCKET_NAME'
            ,default='sfu-storage-dev'
           ,help='Path of the file to read from')
class DataObject:
  def __init__(self,source_bucket_name,processed_bucket_name,source_blob_name):     
    self.source_bucket_name = source_bucket_name 
    self.processed_bucket_name = processed_bucket_name
    self.source_blob_name = source_blob_name


class File_Not_Found(beam.DoFn):
  def start_bundle(self):
    self.storage_client = storage.Client()

  def process(self, element):
    bucket = self.storage_client.get_bucket(PROCESSED_BUCKET_NAME)
    # # Create a new blob and upload the file's content.
    blob = bucket.blob(FileNot_Found_Error+str(datetime.datetime.today())+'.txt')
    blob.upload_from_string("Error log Timestamp:" +str(datetime.datetime.today())+"\nError Details: Associate file is not found in specifed GCS bucket \nError log location :"+Error_log, content_type="text/plain")
    return element 
 
class Empty_File(beam.DoFn):
  def start_bundle(self):
    self.storage_client = storage.Client()

  def process(self, element):
    bucket = self.storage_client.get_bucket(PROCESSED_BUCKET_NAME)
    # Create a new blob and upload the file's content.
    blob = bucket.blob(Empty_FileError+str(datetime.datetime.today())+'.txt')
    blob.upload_from_string("Error log Timestamp:" +str(datetime.datetime.today())+"\nError Details:"+BLOB_NAME2+" file is an empty file \nError log location :"+Error_log, content_type="text/plain")
    return element 


def clean_row(row):
     new_row = []
     #print("original row in clean func",row)
     for i in row:
         #print("inside loop word",i,type(i))
         clean_word = i.replace("*", '')
         #print("clean word",clean_word)
         if clean_word.upper() in ['PASS', 'FAIL', 'ABORT', 'PENDING', 'SKIP', 'CONCESSION-FINAL']:
             new_row.append(clean_word.upper())
         else:
             new_row.append(clean_word)
     #print("clean row in clean func", new_row)
     return new_row

# This ParDo perform record level validation and bifurcation.Segregate the data into correct and error data.          
class CustomParsing(beam.DoFn):    
    # These tags will be used to tag the outputs of this DoFn
    OUTPUT_TAG_CORRECT = 'correctrecords_table'
    OUTPUT_TAG_ERROR = 'errorrecords_table'
    
    
    def __init__(self,data_obj):
        self.data_obj = data_obj
        self.source_blob_name = self.data_obj.source_blob_name
        
        
    def is_not_blank(s):
        return bool(s and not s.isspace())
   
    def process(self, element):
        #print(element)
        from datetime import datetime
        #csv_reader = csv.reader([element], delimiter=',',quoting=csv.QUOTE_NONE)
        csv_reader = [x for x in list(csv.reader([element], delimiter=',', quotechar='"'))]

        # logging.info(BLOB_NAME2)                 
        file_name=Beam_Parameter.GCS_File_location.get()
 
       
        for row in csv_reader:
           row = [d.replace('"', '') for d in row]
           
           
           
            # ********  Attention to development team **************************************
            # ********* START **************************************************************
            # Below code perform all record level custom validations. 
           
           if (len(row))==int(Target_Table_length):
            try:
                          
              if  True: 
                
               #Validate whether Primary keys has values or not
                if(True):
                    from datetime import datetime
                    published_time = datetime.utcnow().isoformat()
                    row.append(published_time)
                    # row.append(file_name)
                    row = clean_row(row)
                    yield beam.pvalue.TaggedOutput(self.OUTPUT_TAG_CORRECT,row)
                    
                # If Primary keys are empty then tag the entire row as Error record    
                else:                                      
                    errorRow={}
                    errorRow["actual_record"]=row
                    errorjson=json.dumps(errorRow)
                                        
                    rowlist=[]
                    rowlist.append("Id")
                    rowlist.append(datetime.utcnow().isoformat())                   
                    rowlist.append(errorjson)
                    rowlist.append("CHW File Report")  
                    rowlist.append("Primary keys are missing or Invalid Datatype")                     
                    
                    
                    yield beam.pvalue.TaggedOutput(self.OUTPUT_TAG_ERROR,rowlist)
                    
                   
                
                                    
            except :
                ex_type, ex_value, ex_traceback = sys.exc_info()
                logging.info("Exception message : %s" %ex_value,400)
           #If length of the input file is nor equal to the length of Table then tag the entire row as Error record
           else: 
                 

                 errorRow={}

                 errorRow["actual_record"]=row

                 errorjson=json.dumps(errorRow)

                 rowlist=[]

                 rowlist.append("Id")

                 rowlist.append(datetime.utcnow().isoformat())                  

                 rowlist.append(errorjson)

                 rowlist.append("CHW File Report")  

                 rowlist.append("Schema Mismatch Error")                    

                 yield beam.pvalue.TaggedOutput(self.OUTPUT_TAG_ERROR,rowlist)            
     
   
            # *********************************** END ***************************************************

#check if file is empty or not
def emptycheck(SOURCE_BUCKET_NAME,BLOB_NAME):

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(SOURCE_BUCKET_NAME)
    blob = bucket.blob(BLOB_NAME)
    blob = blob.download_as_string()
    blob = blob.decode('utf-8')
    blob = StringIO(blob)  #tranform bytes to string here

    names = csv.reader(blob)  #then use csv library to read the content
    rows = list(names)
    
    if (len(rows)) <= 1:
               logging.info(BLOB_NAME+" is an Empty file")
               msg='Empty file'
               return msg
    else:
               logging.info(BLOB_NAME+" is a NonEmpty file")
               msg='NonEmpty file'
               return msg
               
               
def parse_file1(element):
    if element == None:
        return None
    if ((element.upper()=='[NULL]') or element.upper()=='NULL' or element.strip() == ""):

                    element=None
    else:

            pass

    return element
                
#Mapping the sechema 
def validate_json(element,column_list):
    if len(element)!=len(column_list):
        logging.info('FAilure : Column name of table doesnot match with CSV file------')
        return True
    return False

def prepare_dictionary(element):
    column_list=table_schema.split(',')
    if (validate_json(element,column_list)):
     
        exit()
        
    d={}
    for idx,i in enumerate(column_list):
        value='{0}'.format(i.strip().split(':')[0])
        d[value]=parse_file1(element[idx])
       
    return d

def prepare_dictionary1(element):
    column_list=ERROR_TABLE_SCHEMA.split(',')
    if (validate_json(element,column_list)):
        exit()
    d={}
    for idx,i in enumerate(column_list):
        value='{0}'.format(i.strip().split(':')[0])
        d[value]=element[idx]
   
    return d
     


#Moves a blob to NonProcessed folder once Error record is encountered      
def move_nonprocessed_file(SOURCE_BUCKET_NAME,PROCESSED_BUCKET_NAME,BLOB_NAME):
    """Moves a blob from one folder to another with the same name."""
    
    storage_client = storage.Client()

    source_bucket = storage_client.get_bucket(SOURCE_BUCKET_NAME)
    source_blob = source_bucket.blob(BLOB_NAME)
    destination_bucket = storage_client.get_bucket(PROCESSED_BUCKET_NAME)
    source_bucket.copy_blob(source_blob, destination_bucket,  "NonProcessedFiles/" + BLOB_NAME)
    source_blob.delete()  
    
    
#This function will validate if any error record is encountered or not    
def if_error(element, is_empty):
    if is_empty:
        pass
    else:
        import datetime as dt
        storage_client = storage.Client()
        bucket_data = storage_client.get_bucket(PROCESSED_BUCKET_NAME)

        # Create a new blob and upload the file's content.
        my_file = bucket_data.blob(File_DataError+str(dt.datetime.today())+'.txt')
        my_file.upload_from_string("Error log Timestamp:" +str(dt.datetime.today())+", Error Records Encountered in Associate data  : Error Details:Please reffer Error table:"+ERROR_TABLE, content_type="text/plain")
        logging.info('183608:Error Records Encountered in Associate data:    Error Details:Please reffer Error table:{ERROR_TABLE}')
        # Moving the error files from Raw to nonprocessed bucket
        #move_nonprocessed_file(SOURCE_BUCKET_NAME,PROCESSED_BUCKET_NAME,BLOB_NAME2)   
if __name__ == '__main__':

    bq_pipeline = beam.Pipeline(options=PipelineOptions())
    
    
    parser = argparse.ArgumentParser()
    known_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions()
    logger = logging.getLogger().setLevel(logging.INFO)
    p = beam.Pipeline(options=PipelineOptions())
    
    Beam_Parameter = pipeline_options.view_as(DataflowWalmart)
    
    # config = yaml.safe_load(open(Beam_Parameter.config_File))
    # Project_Id=Beam_Parameter.Project_id
    # ENV=Beam_Parameter.ENV
    SOURCE_BUCKET_NAME=Beam_Parameter.SOURCE_BUCKET_NAME
    gcs_file_path=str(Beam_Parameter.GCS_File_location)
    
    # Target_Dataset_Id=config['Target_Dataset_Id'] 
    # Target_table_id=config['Target_Table_Id']
    
    # ERROR_TABLE=config['Error_Table_Name'] 
    # ERROR_TABLE_SCHEMA=config['Error_Table_Schema']
    # Error_log=SOURCE_BUCKET_NAME+'/'+config['Error_Log']
    # FileNot_Found_Error=config['FileNot_Found_Error']
    # Empty_FileError=config['Empty_FileError']
    # File_DataError=config['File_DataError']
    # table_schema=config['Target_Table_Schema']
    # Target_Table_length=config['Target_Table_length']
 
    PROCESSED_BUCKET_NAME=SOURCE_BUCKET_NAME
    
    
    
    # if ENV=='UAT':
    #     Target_Dataset_Id=Target_Dataset_Id+'_'+ENV
    #     Source_Dataset_Id=Source_Dataset_Id+'_'+ENV
            
    #*********************************Process BP Data*********************************************************************************
    
    BLOB_NAME2 = os.path.basename(gcs_file_path)

    data_obj2 = DataObject(SOURCE_BUCKET_NAME,PROCESSED_BUCKET_NAME,BLOB_NAME2)    
    #Building GCS source file path.
    
    
    
    # Read the csv file from GCS bucket
    try:
        gcs_records =( p  | 'Associate' >> beam.io.ReadFromText(Beam_Parameter.GCS_File_location,skip_header_lines = 1))
        processed_records =(gcs_records | "CustomParse_BP" >> beam.ParDo(CustomParsing(data_obj2)).with_outputs(CustomParsing.OUTPUT_TAG_CORRECT,CustomParsing.OUTPUT_TAG_ERROR))
        correct_records= (processed_records[CustomParsing.OUTPUT_TAG_CORRECT] | 'Correct_Records_Dictionary' >> beam.Map(lambda x:prepare_dictionary(x) ))
        correct_records  | 'WriteToBigQueryAssociate_Table' >> beam.io.WriteToBigQuery('{0}:{1}.{2}'.format(Project_Id,Target_Dataset_Id,Target_table_id),schema=table_schema,write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE) 
        error_records = (processed_records[CustomParsing.OUTPUT_TAG_ERROR] | 'Error_Records_Dictionary' >> beam.Map(lambda x: prepare_dictionary1(x)))
        save_error_records=error_records | "Write To BQAssociate FailedRecords" >> beam.io.WriteToBigQuery(ERROR_TABLE,schema=ERROR_TABLE_SCHEMA,write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
        verify_error_record = (error_records | "CountGloballyDupes" >> beam.combiners.Count.Globally()
                               | "Is empty?" >> beam.Map(lambda n: n == 0))
        verify_error_record     | beam.Map(if_error, is_empty=pvalue.AsSingleton(verify_error_record))
                    
    except BaseException as ex: 
        print(ex)
        ex_type, ex_value, ex_traceback = sys.exc_info()
        print("Exception message : %s" %ex_value)
        print("Exception type : %s " % ex_type.__name__)
        print(ex_type.__name__)
        #Generate error log when no input file is present in the bucket
        No_File=(p| beam.Create([None])  | beam.ParDo(File_Not_Found()))

        logging.info('Associate file is not found in specifed GCS bucket => Error log location :{Error_log}')                          

    result = p.run()
    
    result.wait_until_finish()
    
    

    

