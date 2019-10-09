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
![result script](https://yuras-practice.s3.eu-central-1.amazonaws.com/script_result.png?response-content-disposition=inline&X-Amz-Security-Token=AgoJb3JpZ2luX2VjELn%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCmV1LW5vcnRoLTEiRzBFAiEA4zKtFbNrlSF1UW7at6hny%2BSR2P1VxI9zV6gI0o6y%2FvgCIHfy%2Fc4WaYfBzYA7DlGAUtnRR2eXONT%2BRBEz5xily8sKKqYCCKP%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEQABoMNjQ4OTMwNDI2ODUwIgyj46MVlJm1R5bwAF0q%2BgHtJ211DbajIjsrH0OtnLSrZCifMhAWu4gY7C0Do0q2hEmFYYtEbYxpeDDaYHA5qOo7rR%2FRJsnIbrjlRfyhIgPuIWqrGv7G8APsS%2Fi5Iw5uuPby%2BDYY%2Fg96eHOybW5Af%2F0nQzD9PIII27zPhpI7iBQPBDiEGw6kvze3D124G0jLl6Rl6lr8ybXC4s5rYWU42DA9v%2FjbW4bzOOQ45NIvoLts71Eglklf6s%2BiudSL3xd15QPVTltMDzCVVR0TBfyaWSQUyA9ikSnP6ejOno949A9g%2FpeCqKSDedBWF15FHHWaxP37KXUMDSVtNFDFu3QA5k0o%2B0qeZMIkOtPGMOHN9uwFOrQBgnGQyyolhVtsBGxc0jyRdENeYQqnRNUpHutDu0n7wfT%2Ft5np9N0pVZ2yoLTmsZLEzQiyyoAHdU68yLhSUimtzq6CEP8Ts87MhTnQRqjHntSwaXApLjKqUuxE72JVOk0OPTiJpKXRVA5icxIdg9vB15JCoq1qQB171NMts8zA4EIoErBrlpDJ2rnZc3LH2s9ZWI8lFoLIGaBK%2ByAh%2BaYarru0cRqwMb3UTzvNoN8n8EAc8YsE&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20191009T092620Z&X-Amz-SignedHeaders=host&X-Amz-Expires=300&X-Amz-Credential=ASIAZOF2EP7RBDKB2BE5%2F20191009%2Feu-central-1%2Fs3%2Faws4_request&X-Amz-Signature=7fd2c3bd7bc6f2f5aae23c2dbe35745708044c2dc7379041bec19e1f646e0265)
    
### Requirments:
* docker>=4.1.0
* ndjson>=0.2.0
* psycopg2-binary>=2.8.3
* pytest>=5.2.1
* requests>=2.22.0


#### Issues that require further improvement:
* DB population productivity
* Decorator may be used for creating connection with DB
