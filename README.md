### Description:
Script starts a Python 3 application 
that retrieves the NDJSON files containing 
FHIR demo resources from git repository and 
uses their contents to populate the postgresql 
database according to the transformation rules.


### Script start:

* `clone repository`
* `cd healthcare`
* `python3 -m venv venv`
* `source venv/bin/activate`
* `sh run.sh`

After this steps a new postresql db in docker 
container will appear with four tables that
are populated with sample data and some 
information about content is going to be
printed. If data to be populated does not have 
obligatory field it will be not saved in db but 
saved in `logs/<filename>.ndjson`

**Result of script implementation:**
![result script](https://yuras-practice.s3.eu-central-1.amazonaws.com/script_result.png)
    
### Requirments:
* docker>=4.1.0
* ndjson>=0.2.0
* psycopg2-binary>=2.8.3
* pytest>=5.2.1
* requests>=2.22.0
* Docker Desktop 


#### Issues that require further improvement:
* DB population productivity
* Decorator may be used for creating connection with DB
